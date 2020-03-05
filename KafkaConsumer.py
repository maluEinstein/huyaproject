# kafka消费者测试
from kafka import KafkaConsumer

consumer = KafkaConsumer('2020030511')
for msg in consumer:
    print(msg.value.decode('utf8'))
