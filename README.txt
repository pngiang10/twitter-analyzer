in kafka folder: 

start zookeeper
./bin/zookeeper-server-start.sh config/zookeeper.properties

start server
./bin/kafka-server-start.sh config/server.properties

create topic: 1 for <input kafka topics> and 1 for <ouput kafka topics>
./bin/kafka-topics.sh --create --topic stream2 --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

start producer with <input kafka topics> (optional)
producer
./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test

start consumer with <input kafka topics>
consumer
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test

start consumer with <output kafka topics>
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic analyze

start connector
bin/connect-standalone.sh config/connect-standalone.properties config/elasticsearch-connect.properties

---------------------------------------------------
in p3 folder:

1.
strm.py: to stream tweets to kafka: 
arguments: <hostserver> <input kafka topics> <search terms> <language>
CMD: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 strm.py <hostserver> <input kafka topics> <search terms> <language>
EX:  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 strm.py localhost:9092 stream covid-19 en

2.
anlz.py: to analyze tweets then write back to kafka
arguments: <hostserver> <input kafka topics> <ouput kafka topics>
CMD: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 anlz.py <hostserver> <input kafka topics> <ouput kafka topics>
EX:  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 anlz.py localhost:9092 stream analyze

