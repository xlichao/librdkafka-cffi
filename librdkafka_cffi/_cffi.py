# librdkafka_cffi/_cffi.py
import logging
import sys

from ._librdkafka_cffi import ffi, lib
from .errors import (
    KafkaException, KafkaConfigException, KafkaProduceException,
    KafkaConsumeException, KafkaSupportException, KafkaTimeoutError
)
from .futures import RecordMetadata

# Setup logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO) # Default level, can be configured by user

# Map librdkafka log levels to Python logging levels
_LOG_LEVEL_MAP = {
    0: logging.NOTSET,    # RD_KAFKA_LOG_EMERG
    1: logging.CRITICAL,  # RD_KAFKA_LOG_ALERT
    2: logging.CRITICAL,  # RD_KAFKA_LOG_CRIT
    3: logging.ERROR,     # RD_KAFKA_LOG_ERR
    4: logging.WARNING,   # RD_KAFKA_LOG_WARNING
    5: logging.INFO,      # RD_KAFKA_LOG_NOTICE
    6: logging.INFO,      # RD_KAFKA_LOG_INFO
    7: logging.DEBUG,     # RD_KAFKA_LOG_DEBUG
}

@ffi.def_extern()
def log_callback(rk, level, fac, buf):
    """
    librdkafka log callback. Routes librdkafka logs to Python's logging system.
    """
    try:
        log_level = _LOG_LEVEL_MAP.get(level, logging.INFO)
        message = f"[{ffi.string(fac).decode('utf-8')}] {ffi.string(buf).decode('utf-8')}"
        logger.log(log_level, message)
    except Exception as e:
        logger.error(f"Error in librdkafka log_callback: {e}", exc_info=True)


# Dictionary to hold references to Python objects keyed by their unique integer ID
_opaque_id_map = {}
_next_opaque_id = 0

def get_next_opaque_id():
    global _next_opaque_id
    current_id = _next_opaque_id
    _next_opaque_id += 1
    return current_id

@ffi.def_extern()
def rebalance_callback(rk, err, partitions, opaque):
    """
    librdkafka rebalance callback.
    This function is called by librdkafka when a rebalance occurs.
    """
    try:
        if opaque == ffi.NULL:
            logger.warning("Rebalance callback received with NULL opaque pointer. Cannot call Python consumer methods.")
            # We cannot retrieve the Python consumer, so we cannot call its rebalance methods.
            # This is expected if the producer is not setting conf opaque.
            # However, librdkafka might still expect a response to assign/unassign.
            # For now, just log and return. More robust handling might be needed here.
            return
            
        consumer = ffi.from_handle(opaque) # Retrieve Python KafkaConsumer object
        
        if err == lib.RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
            logger.info("Assign partitions triggered.")
            # Call the Python consumer's assign method
            consumer._rebalance_assign(partitions)
        elif err == lib.RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
            logger.info("Revoke partitions triggered.")
            # Call the Python consumer's revoke method
            consumer._rebalance_revoke(partitions)
        else:
            logger.error(f"Rebalancing error: {ffi.string(lib.rd_kafka_err2str(err)).decode('utf-8')}")
            # Handle other errors or pass them to the consumer's error callback
    except Exception as e:
        logger.error(f"Error in rebalance_callback: {e}", exc_info=True)


@ffi.def_extern()
def delivery_report_callback(rk, rkmessage, opaque):
    """
    librdkafka delivery report callback.
    This function is called once for each message produced to indicate
    delivery status. The `opaque` parameter is a handle to the
    FutureRecordMetadata object created by the send() method.
    """
    try: # Outer try-except for robust error logging
        # Retrieve future_id from rkmessage._private (which was the opaque_id cast to void *)
        future_id = ffi.cast("uintptr_t", rkmessage._private)
        
        # Retrieve producer_id from rk's opaque
        producer_id = ffi.cast("uintptr_t", lib.rd_kafka_opaque(rk))
        
        # Get the producer instance to remove the future from its pending set
        producer = _opaque_id_map[producer_id]
        
        # Get the future object and remove it from the map
        future = _opaque_id_map.pop(future_id)

        if rkmessage.err == lib.RD_KAFKA_RESP_ERR_NO_ERROR:
            # Message delivered successfully
            topic_name = ffi.string(lib.rd_kafka_topic_name(rkmessage.rkt)).decode('utf-8')
            metadata = RecordMetadata(
                topic=topic_name,
                partition=rkmessage.partition,
                offset=rkmessage.offset
            )
            future.set_result(metadata)
        else:
            # Message delivery failed
            err_str = ffi.string(lib.rd_kafka_err2str(rkmessage.err)).decode('utf-8')
            kafka_exception = KafkaProduceException(
                f"Message delivery failed: {err_str}",
                error_code=rkmessage.err
            )
            future.set_exception(kafka_exception)

    except Exception as e:
        logger.error(f"FATAL: Unhandled error in delivery_report_callback: {e}", exc_info=True)
    finally:
        # No ffi.release needed here as we manage objects via _opaque_id_map
        pass

def _check_kafka_error(err_code, message=None):
    """
    Checks a librdkafka error code and raises a KafkaException if it indicates an error.
    """
    try:
        if err_code == 0:  # RD_KAFKA_RESP_ERR_NO_ERROR
            return

        err_str = ffi.string(lib.rd_kafka_err2str(err_code)).decode('utf-8')
        full_message = f"{message if message else 'librdkafka error'}: {err_str} (Error Code: {err_code})"

        # Raise specific exceptions for common error codes
        if err_code == lib.RD_KAFKA_RESP_ERR__MSG_TIMED_OUT:
            raise KafkaTimeoutError(full_message, err_code)
        # Add more specific error mappings here as needed

        raise KafkaException(full_message)
    except Exception as e:
        logger.error(f"Error in _check_kafka_error: {e}", exc_info=True)
        raise # Re-raise to ensure the error propagates

def _get_kafka_error_string(err_code):
    """
    Returns the string representation of a librdkafka error code.
    """
    return ffi.string(lib.rd_kafka_err2str(err_code)).decode('utf-8')

# Expose constants that might be useful
NO_ERROR = lib.RD_KAFKA_RESP_ERR_NO_ERROR
MSG_TIMED_OUT = lib.RD_KAFKA_RESP_ERR__MSG_TIMED_OUT