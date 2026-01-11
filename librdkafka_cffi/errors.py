# librdkafka_cffi/errors.py

class KafkaException(Exception):
    """Base exception for librdkafka-cffi errors."""
    pass

class KafkaConfigException(KafkaException):
    """Exception raised for librdkafka configuration errors."""
    pass

class KafkaProduceException(KafkaException):
    """Exception raised when a message fails to produce."""
    def __init__(self, message, error_code=None):
        super().__init__(message)
        self.error_code = error_code

class KafkaConsumeException(KafkaException):
    """Exception raised when there's an error during message consumption."""
    def __init__(self, message, error_code=None):
        super().__init__(message)
        self.error_code = error_code

class KafkaSupportException(KafkaException):
    """Exception raised for unsupported librdkafka features."""
    pass

class KafkaTimeoutError(KafkaException):
    """Exception raised when a librdkafka operation times out."""
    pass

# Add more specific exceptions as needed
