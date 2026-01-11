# tests/test_producer.py
import pytest
import time
from librdkafka_cffi import KafkaProducer, KafkaException, KafkaConfigException, NO_ERROR

# Assume a Kafka broker is running at localhost:9092 for these tests

@pytest.fixture(scope="module")
def kafka_broker_address():
    # This can be configured via environment variable or a config file
    return "localhost:9092"

@pytest.fixture
def producer_config(kafka_broker_address):
    return {
        "bootstrap.servers": kafka_broker_address,
        "acks": "all",
        "message.timeout.ms": "5000"
    }

def test_producer_init_success(producer_config):
    producer = None
    try:
        producer = KafkaProducer(producer_config)
        assert producer is not None
        assert producer.rk is not ffi.NULL
    finally:
        if producer:
            producer.close()

def test_producer_init_fail(kafka_broker_address):
    # Test with invalid bootstrap.servers to ensure it fails gracefully
    invalid_config = {
        "bootstrap.servers": "invalidhost:9092",
        "acks": "all"
    }
    with pytest.raises(KafkaException):
        producer = KafkaProducer(invalid_config)

class DeliveryReport:
    def __init__(self):
        self.delivered = False
        self.error = None

    def callback(self, producer, error, opaque):
        self.delivered = True
        self.error = error

def test_producer_produce_success(producer_config):
    producer = None
    delivery_report = DeliveryReport()
    test_topic = "test_topic_producer_success"
    test_value = b"hello_kafka"
    test_key = b"test_key"
    try:
        producer = KafkaProducer(producer_config)
        producer.produce(test_topic, value=test_value, key=test_key, opaque=delivery_report)
        producer.flush(10000) # Wait up to 10 seconds for delivery reports

        assert delivery_report.delivered is True
        assert delivery_report.error is None

    finally:
        if producer:
            producer.close()

def test_producer_produce_no_value(producer_config):
    producer = None
    delivery_report = DeliveryReport()
    test_topic = "test_topic_producer_no_value"
    test_key = b"test_key_no_value"
    try:
        producer = KafkaProducer(producer_config)
        producer.produce(test_topic, value=None, key=test_key, opaque=delivery_report)
        producer.flush(10000) # Wait up to 10 seconds for delivery reports

        assert delivery_report.delivered is True
        assert delivery_report.error is None

    finally:
        if producer:
            producer.close()

def test_producer_flush_timeout(producer_config):
    producer = None
    try:
        producer_config["message.timeout.ms"] = "100" # Very short timeout for message production
        producer = KafkaProducer(producer_config)
        test_topic = "test_topic_producer_flush_timeout"
        producer.produce(test_topic, value=b"timeout_message")
        
        # Flush with a very short timeout, expecting it to fail to deliver
        with pytest.raises(KafkaException) as excinfo:
            producer.flush(100) # 100ms flush timeout

        assert "Failed to flush producer" in str(excinfo.value)
        # The specific error code might vary depending on the exact failure
        # assert excinfo.value.error_code == NO_ERROR # This would be a specific librdkafka error
    finally:
        if producer:
            producer.close()

