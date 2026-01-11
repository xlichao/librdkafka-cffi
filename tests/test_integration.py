# -*- coding: utf-8 -*-
import unittest
import time
import uuid
from typing import Optional

from librdkafka_cffi.Producer import Producer
from librdkafka_cffi.Consumer import Consumer, Message


class TestIntegration(unittest.TestCase):
    def test_produce_consume(self):
        """
        测试完整的生产和消费流程。
        1. 创建一个生产者并发送一条唯一的消息。
        2. 创建一个消费者并订阅相应的主题。
        3. 轮询消息，直到收到发送的消息或超时。
        4. 验证接收到的消息与发送的消息完全一致。
        """
        conf = {'bootstrap.servers': 'localhost:9092'}
        
        # 等待 Kafka 容器准备就绪
        wait_for_kafka_timeout = 30
        start_time = time.time()
        kafka_ready = False
        while time.time() - start_time < wait_for_kafka_timeout:
            try:
                # 尝试创建一个临时生产者来检查连接
                p = Producer({'bootstrap.servers': 'localhost:9092', 'socket.timeout.ms': '5000'})
                p.list_topics(timeout=5.0) # list_topics 是一个很好的连接检查方法
                p.close()
                kafka_ready = True
                print("Kafka is ready.")
                break
            except Exception as e:
                print(f"Waiting for Kafka to be ready... ({e})")
                time.sleep(2)
        
        if not kafka_ready:
            self.fail("Kafka container did not become ready in time.")

        # 使用UUID确保每次测试的主题和消息都是唯一的
        topic_name = f'test-topic-{uuid.uuid4()}'
        message_key = f'test-key-{uuid.uuid4()}'.encode('utf-8')
        message_value = f'test-value-{uuid.uuid4()}'.encode('utf-8')

        # 1. 生产消息
        producer = Producer(conf)
        producer.produce(topic_name, value=message_value, key=message_key)
        producer.flush(10)  # 等待消息发送完成

        # 2. 消费消息
        consumer_conf = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': f'test-group-{uuid.uuid4()}',
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(consumer_conf)
        consumer.subscribe([topic_name])

        # 3. 轮询并验证消息
        received_message: Optional[Message] = None
        start_time = time.time()
        timeout = 30  # 30秒超时

        while time.time() - start_time < timeout:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                # 如果是真实的错误而不是分区末尾，则测试失败
                from librdkafka_cffi.errors import KafkaError
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    self.fail(f"消费过程中发生错误: {msg.error()}")
            else:
                received_message = msg
                break
        
        consumer.close()

        # 4. 断言
        self.assertIsNotNone(received_message, "在规定时间内未收到消息")
        self.assertEqual(received_message.key(), message_key)
        self.assertEqual(received_message.value(), message_value)
        self.assertEqual(received_message.topic(), topic_name)


if __name__ == '__main__':
    unittest.main()
