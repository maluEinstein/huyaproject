from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
import json, time, os

broker_list = "test"
topic_name = "huya_crawler"
timer = 5
offsetRanges = []


