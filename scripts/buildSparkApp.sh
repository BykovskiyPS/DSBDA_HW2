#!/bin/bash
echo "Creating .jar file..."

mvn package -f ../pom.xml
cp ../target/lab2-1.0-SNAPSHOT-jar-with-dependencies.jar /tmp

# Скачиваем kafka
if [ ! -f kafka_2.13-2.8.0.tgz ]; then
    wget https://apache-mirror.rbc.ru/pub/apache/kafka/2.8.0/kafka_2.13-2.8.0.tgz
    tar xvzf kafka_2.13-2.8.0.tgz
else
    echo "Kafka already exists, skipping..."
fi

# Скачиваем Spark
if [ ! -f spark-2.3.1-bin-hadoop2.7.tgz ]; then
    wget https://archive.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz
    tar xvzf spark-2.3.1-bin-hadoop2.7.tgz
else
    echo "Spark already exists, skipping..."
fi
