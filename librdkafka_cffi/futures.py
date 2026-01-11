# librdkafka_cffi/futures.py
from concurrent.futures import Future

class RecordMetadata:
    """
    A class to hold metadata for a successfully produced record.
    """
    def __init__(self, topic, partition, offset):
        self.topic = topic
        self.partition = partition
        self.offset = offset

    def __repr__(self):
        return f"RecordMetadata(topic='{self.topic}', partition={self.partition}, offset={self.offset})"

class FutureRecordMetadata(Future):
    """
    A Future that resolves to a RecordMetadata object on successful message production.
    """
    def __init__(self):
        super().__init__()
        self.topic = None
        self.partition = None
        self.offset = None

    def set_result(self, result: RecordMetadata):
        """
        Sets the result of the future to a RecordMetadata object.
        """
        self.topic = result.topic
        self.partition = result.partition
        self.offset = result.offset
        super().set_result(result)

    def get(self, timeout=None):
        """
        Alias for result() for compatibility with kafka-python.
        """
        return self.result(timeout=timeout)
