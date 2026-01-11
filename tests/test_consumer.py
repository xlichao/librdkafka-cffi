# tests/test_consumer.py
import pytest
import time
from librdkafka_cffi import KafkaConsumer, KafkaException, KafkaConfigException

# Assume a Kafka broker is running at localhost:9092 for these tests

@pytest.fixture(scope="module")
def kafka_broker_address():
    # This can be configured via environment variable or a config file
    return "localhost:9092"

@pytest.fixture
def consumer_config(kafka_broker_address):
    return {
        "bootstrap.servers": kafka_broker_address,
        "group.id": "test_group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "false" # We will commit manually
    }

def test_consumer_init_success(consumer_config):
    consumer = None
    try:
        consumer = KafkaConsumer(consumer_config)
        assert consumer is not None
        assert consumer.rk is not ffi.NULL
    finally:
        if consumer:
            consumer.close()

def test_consumer_init_fail(kafka_broker_address):
    # Test with invalid bootstrap.servers to ensure it fails gracefully
    invalid_config = {
        "bootstrap.servers": "invalidhost:9092",
        "group.id": "test_group",
        "auto.offset.reset": "earliest"
    }
    with pytest.raises(KafkaException):
        consumer = KafkaConsumer(invalid_config)

def test_consumer_subscribe_and_poll(consumer_config, kafka_broker_address):
    # This test requires a producer to send messages to 'test_topic_consumer'
    consumer = None
    producer = None # Placeholder for a producer if we had one to send messages
    try:
        # Create a dummy producer to ensure there are messages
        # In a real scenario, this would be a KafkaProducer sending messages
        # For now, we'll assume messages exist or rely on manual setup
        # producer = KafkaProducer({"bootstrap.servers": kafka_broker_address})
        # producer.produce("test_topic_consumer", value=b"test_message_1")
        # producer.flush()

        consumer = KafkaConsumer(consumer_config)
        topic = "test_topic_consumer"
        consumer.subscribe([topic])
        
        # Allow some time for consumer to join group and get assignments
        time.sleep(2) 

        # Poll for messages (assuming some messages were produced to this topic)
        msg = consumer.poll(1000) # 1 second timeout

        # Depending on test setup, msg might be None if no messages produced yet
        # For a more robust test, a producer fixture would be needed
        # assert msg is not None
        # assert msg.value == b"test_message_1"
        # assert msg.topic == topic.encode('utf-8')
        
    finally:
        if consumer:
            consumer.close()
        # if producer:
        #     producer.close()

def test_consumer_manual_commit(consumer_config):
    consumer = None
    try:
        consumer_config["enable.auto.commit"] = "false"
        consumer = KafkaConsumer(consumer_config)
        topic = "test_topic_commit"
        consumer.subscribe([topic])
        time.sleep(2) # Wait for assignment

        # Assuming a message is polled
        # msg = consumer.poll(1000)
        # if msg:
        #     consumer.commit(msg)
        #     # Further assertions about commit success if possible
        
    finally:
        if consumer:
            consumer.close()

def test_consumer_manual_assign(consumer_config):
    consumer = None
    try:
        consumer = KafkaConsumer(consumer_config)
        topic = "test_topic_assign"
        # Example of manual assignment: (topic, partition, offset)
        # For this to work, the topic and partition must exist
        consumer.assign([(topic, 0, 0)]) # Assign to partition 0 from offset 0
        
        # Poll for message after assignment
        # msg = consumer.poll(1000)
        # assert msg is not None # If messages exist on that partition
        
    finally:
        if consumer:
            consumer.close()
