create_topic:
	docker exec -it clickhouse_avro_kafka_1 /usr/bin/kafka-topics --zookeeper zookeeper:2181 --create --partitions 1 --replication-factor 1 --topic atopic

insert_data:
	for i in `seq 100`; do echo "{}:{}"; done | docker run -i --net clickhouse_avro_default edenhill/kafkacat:1.6.0 kafkacat -b kafka:9092 -P -t atopic -K:

list_topics:
	docker exec -it clickhouse_avro_kafka_1 /usr/bin/kafka-topics --zookeeper zookeeper:2181 --list

consume_data:
	docker run -it --net clickhouse_avro_default edenhill/kafkacat:1.6.0 kafkacat -b kafka:9092  -t atopic -f 'Topic %t[%p], offset: %o, key: %k, payload: %S bytes: %s\n'	

start_connect:
	docker exec -it clickhouse_avro_kafka_1 bash /iceberg/bin/debug.sh	

start:
	docker exec -it clickhouse_avro_kafka_1 bash 
