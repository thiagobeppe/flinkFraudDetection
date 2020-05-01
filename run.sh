#!/bin/bash
sudo apt install figlet -y;
figlet Docker!;
docker-compose up -d;
gnome-terminal  -- bash -c "figlet -c Kafka;cd fraudDetection; sbt \"runMain com.github.example.kafka.producerKafka\"";
gnome-terminal  -- bash -c "figlet -c Consumidor Flink;cd fraudDetection; sbt \"runMain com.github.example.fraudDetection\""
