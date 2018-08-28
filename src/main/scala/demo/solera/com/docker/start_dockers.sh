#!/bin/bash

docker run --rm -d --name=zk -p 2181:2181 31z4/zookeeper

ZOOKEEPER_QUORUM_IP=$(docker inspect -f "{{ .NetworkSettings.IPAddress }}" zk)

docker run --rm -d --name=kafka -p 9092:9092 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
-e KAFKA_ZOOKEEPER_CONNECT=${ZOOKEEPER_QUORUM_IP}:2181 -e KAFKA_BROKER_ID=1 confluent/kafka:0.10.0.0-cp1

sleep 5

docker exec -d kafka /usr/bin/kafka-topics --create --zookeeper ${ZOOKEEPER_QUORUM_IP}:2181 --replication-factor 1 --partitions 1 --topic test

docker run --rm -d --name=hadoop -p 8020:8020 -p 8032:8032 -p 8088:8088 -p 9000:9000 -p 10020:10020 \
-p 19888:19888 -p 50010:50010 -p 50020:50020 -p 50070:50070 -p 50075:50075 -p 50090:50090 \
harisekhon/hadoop

sleep 5

docker exec -d hadoop hadoop fs -chmod 777 /

mkdir -p /tmp/spark-test

echo "districtId,csv_description,total
456465,description,6546546
21,description,46645
21,description,46645
" >> /tmp/spark-test/file