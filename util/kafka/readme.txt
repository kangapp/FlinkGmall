#说明
这是一个kafka工具，包含kafka和zookeeper，提供内部和外部连接

#启动
docker-compose up -d

#内部连接
docker exec -it container_id /bin/bash
kafka-console-producer.sh --broker-list kafka:9092 --topic test
kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic test --from-beginning

#外部连接
kafka-console-producer.sh --broker-list 127.0.0.1:9093 --topic test
kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9093 --topic test --from-beginning