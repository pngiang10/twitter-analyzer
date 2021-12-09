import tweepy
import sys
from tweepy.streaming import Stream
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer

consumer_key = ""
consumer_secret = ""
access_token = ""
access_secret = ""

#bootstrapServers = 'localhost:9092'
bootstrapServers = sys.argv[1]
#topics = 'test'
topics = sys.argv[2]
searchTerm = sys.argv[3]
language = sys.argv[4]

producer = KafkaProducer(bootstrap_servers=bootstrapServers)
future = producer.send(topics, b'raw_bytes')
try:
    record_metadata = future.get(timeout=10)
except KafkaError:
    log.exception()
    pass

class Printer(Stream):
    def on_status(self, status):
        #print(status.text)
        print("streaming")
        producer.send(topics, bytes(status.text, "utf-8"))

printer = Printer(consumer_key, consumer_secret, access_token, access_secret)

if __name__ == "__main__":
    printer.filter(track=[searchTerm], languages=[language])
