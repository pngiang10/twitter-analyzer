#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import sys
#import pandas as pd
#import json
import time

from pyspark.sql.functions import *
#from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from textblob import TextBlob
from pyspark.sql import SparkSession
#from pyspark import SparkContext
#sc =SparkContext()
#from pyspark.sql import SQLContext
#sqlContext = SQLContext(sc)

if __name__ == "__main__":
    bootstrapServers = sys.argv[1]
    subscribeType = "subscribe"
    inTopics = sys.argv[2]
    outTopics = sys.argv[3]

    #bootstrapServers = "localhost:9092"
    #subscribeType = "subscribe"
    #topics = "test"

    spark = SparkSession\
        .builder\
        .appName("StructuredKafkaWordCount")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Create DataSet representing the stream of input lines from kafka
    lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option(subscribeType, inTopics)\
        .option("failOnDataLoss", "false")\
        .load()\
        .selectExpr("CAST(value AS STRING)")

    def clean_tweet(lines):
        words = lines.select(
            explode(
                # plit(lines.value, '')
                split(lines.value, '\n')
            ).alias('word')
            # explode turns each item in an array into a separate row
        )
        words = words.na.replace('', None)
        words = words.na.drop()
        words = words.withColumn('word', F.regexp_replace('word', r'http\S+', ''))
        words = words.withColumn('word', F.regexp_replace('word', '@\w+', ''))
        words = words.withColumn('word', F.regexp_replace('word', '#', ''))
        words = words.withColumn('word', F.regexp_replace('word', 'RT', ''))
        words = words.withColumn('word', F.regexp_replace('word', ':', ''))
        return words


    def get_polarity(text):
        sentiment = TextBlob(text).sentiment.polarity
        if (sentiment>0):
            var1 = 1
            var2 = "positive"
        elif sentiment==0:
            var1 = 0
            var2 = "neutral"
        else:
            var1 = -1
            var2 = "negative"
        var3 = int(time.time()) * 1000
        return("""{ "schema": { "type": "struct", "fields": [{ "type": "int32", "optional": false, "field": "c1" }, { "type": "string", "optional": false, "field": "c2" }, { "type": "int64", "optional": false, "name": "org.apache.kafka.connect.data.Timestamp", "version": 1, "field": "@timestamp" }, { "type": "int64", "optional": false, "name": "org.apache.kafka.connect.data.Timestamp", "version": 1, "field": "update_ts" }], "optional": false, "name": "foobar" }, "payload": { "c1": %s, "c2": "%s", "@timestamp": %s, "update_ts": %s } }"""%(var1, var2, var3, var3))

    print("streaming tweet sentiment to kafka")

    def get_tweet_sentiment(tweet_text):
        # Utility function to classify sentiment of passed tweet
        # using textblob's sentiment method
        get_polarity_udf = udf(get_polarity, StringType())
        # polarity
        #   > 0: positive,
        #   ==0: neutral,
        #   < 0: negative
        tweet_text = tweet_text.withColumn('value', get_polarity_udf("word"))
        return tweet_text

    # Getting tweet sentiment
    words = clean_tweet(lines)
    words = get_tweet_sentiment(words)

    #Writing output to kafka
    query = words \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrapServers) \
        .option("topic", outTopics) \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .start()

    query.awaitTermination()
