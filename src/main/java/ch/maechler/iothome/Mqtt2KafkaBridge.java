package ch.maechler.iothome;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.apache.logging.log4j.*;
import org.eclipse.paho.client.mqttv3.*;
import java.util.Optional;
import java.util.Properties;

public class Mqtt2KafkaBridge implements MqttCallbackExtended {
    private final Logger logger = LogManager.getLogger(Mqtt2KafkaBridge.class);
    private KafkaProducer<String, byte[]> kafkaProducer;
    private MqttClient mqttClient;
    private String mqttTopicSeparator, kafkaTopicSeparator, mqttTopicFilter;

    public static void main(String[] args) {
        new Mqtt2KafkaBridge().run();
    }

    private void run() {
        logger.info("Start to run Mqtt2KafkaBridge.");

        var kafkaProducerProperties = new Properties();
        var clientId = Optional.ofNullable(System.getenv("CLIENT_ID")).orElse("Mqtt2KafkaBridge");
        var kafkaHost = Optional.ofNullable(System.getenv("KAFKA_BROKER_HOST")).orElse("localhost:9092");
        var mqttBrokerHost = Optional.ofNullable(System.getenv("MQTT_BROKER_HOST")).orElse("localhost:1883");
        var mqttBrokerUser = Optional.ofNullable(System.getenv("MQTT_BROKER_USER")).orElse("");
        var mqttBrokerPassword = Optional.ofNullable(System.getenv("MQTT_BROKER_PASSWORD")).orElse("");
        var mqttBrokerAutomaticReconnect = Boolean.parseBoolean(Optional.ofNullable(System.getenv("MQTT_BROKER_AUTOMATIC_RECONNECT")).orElse("true"));
        mqttTopicFilter = Optional.ofNullable(System.getenv("MQTT_TOPIC_FILTER")).orElse("#");
        mqttTopicSeparator = Optional.ofNullable(System.getenv("MQTT_TOPIC_SEPARATOR")).orElse("/");
        kafkaTopicSeparator = Optional.ofNullable(System.getenv("KAFKA_TOPIC_SEPARATOR")).orElse(".");

        logger.info(
            "Configuration values: \n CLIENT_ID={} \n KAFKA_HOST={} \n KAFKA_TOPIC_SEPARATOR={} \n MQTT_BROKER_HOST={} \n MQTT_BROKER_USER={} \n MQTT_BROKER_AUTOMATIC_RECONNECT={} \n MQTT_TOPIC_SEPARATOR={} \n MQTT_TOPIC_FILTER={}",
                clientId, kafkaHost, kafkaTopicSeparator, mqttBrokerHost, mqttBrokerUser, mqttBrokerAutomaticReconnect, mqttTopicSeparator, mqttTopicFilter
        );

        kafkaProducerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
        kafkaProducerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        kafkaProducerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProducerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        try {
            logger.info("Connecting to MQTT and Kafka broker.");

            var mqttConnectOptions = new MqttConnectOptions();
            mqttClient = new MqttClient("tcp://" + mqttBrokerHost, clientId);
            kafkaProducer = new KafkaProducer<>(kafkaProducerProperties);

            mqttConnectOptions.setUserName(mqttBrokerUser);
            mqttConnectOptions.setPassword(mqttBrokerPassword.toCharArray());
            mqttConnectOptions.setAutomaticReconnect(mqttBrokerAutomaticReconnect);

            mqttClient.setCallback(this);
            mqttClient.connect(mqttConnectOptions);

            logger.info("Connected to broker, ready to bridge!");
        } catch(MqttException e) {
            logger.error("Oops, an error occurred.", e);
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        logger.error("Connection to MQTT server lost.", cause);
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        logger.debug("MQTT message arrived: {} -> {} [{}]", topic, message.getId(), message);

        var kafkaTopic = topic.replace(mqttTopicSeparator, kafkaTopicSeparator);
        var producerRecord = new ProducerRecord<String, byte[]>(kafkaTopic, message.getPayload());
        Callback callback = (RecordMetadata metadata, Exception e) -> {
            if (e == null) {
                logger.trace("Message sent to Kafka: {} -> {}", kafkaTopic, message.getId());
            } else {
                logger.error("Oops, an error occurred while sending message to Kafka. \n {} -> {}", kafkaTopic, message.getId(), e);
            }
        };

        kafkaProducer.send(producerRecord, callback);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        logger.warn("Delivery complete. This method should actually never be called. Token: {}", token);
    }

    @Override
    public void connectComplete(boolean reconnect, String serverURI) {
        logger.info("Connect to MQTT broker '{}' complete. Reconnect={}", serverURI, reconnect);

        try {
            logger.info("Subscribe to topic {}.", mqttTopicFilter);
            mqttClient.subscribe(mqttTopicFilter);
        } catch(MqttException e) {
            logger.error("Oops, an error occurred.", e);
        }
    }
}