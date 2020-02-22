from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
import time
import os
import json

broker_list = "xxxx"
topic_name = "xxxx"
timer = 5
offsetRanges = []


def store_offset_ranges(rdd):
    global offsetRanges
    offsetRanges = rdd.offsetRanges()
    return rdd


def save_offset_ranges(rdd):
    root_path = os.path.dirname(os.path.realpath(__file__))
    record_path = os.path.join(root_path, "offset.txt")
    data = dict()
    f = open(record_path, "w")
    for o in offsetRanges:
        data = {"topic": o.topic, "partition": o.partition, "fromOffset": o.fromOffset, "untilOffset": o.untilOffset}
    f.write(json.dumps(data))
    f.close()


def deal_data(rdd):
    data = rdd.collect()
    for d in data:
        # do something
        pass


def save_by_spark_streaming():
    root_path = os.path.dirname(os.path.realpath(__file__))
    record_path = os.path.join(root_path, "offset.txt")
    from_offsets = {}
    # 获取已有的offset，没有记录文件时则用默认值即最大值
    if os.path.exists(record_path):
        f = open(record_path, "r")
        offset_data = json.loads(f.read())
        f.close()
        if offset_data["topic"] != topic_name:
            raise Exception("the topic name in offset.txt is incorrect")

        topic_partion = TopicAndPartition(offset_data["topic"], offset_data["partition"])
        from_offsets = {topic_partion: int(offset_data["untilOffset"])}  # 注意设置起始offset时的方法
        print("start from offsets: %s" % from_offsets)

    sc = SparkContext(appName="Realtime-Analytics-Engine")
    ssc = StreamingContext(sc, int(timer))

    kvs = KafkaUtils.createDirectStream(ssc=ssc, topics=[topic_name], fromOffsets=from_offsets,
                                        kafkaParams={"metadata.broker.list": broker_list})
    kvs.foreachRDD(lambda rec: deal_data(rec))
    kvs.transform(store_offset_ranges).foreachRDD(save_offset_ranges)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()




if __name__ == '__main__':
    save_by_spark_streaming()
