FROM openjdk:11-jre-slim
ADD build/libs/iot-home.mqtt2kafkabridge-1.0.jar /opt/mqtt2kafkabridge/mqtt2kafkabridge.jar
WORKDIR /opt/mqtt2kafkabridge/
CMD java -jar mqtt2kafkabridge.jar