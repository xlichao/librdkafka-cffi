# librdkafka_cffi/Producer.py
import re
import threading
import time
from typing import Dict, Optional, List, Tuple

import librdkafka_cffi._cffi as _cffi_module
from ._cffi import ffi, lib, _check_kafka_error
import librdkafka_cffi._cffi as _cffi
from .errors import KafkaConfigException, KafkaProduceException, KafkaTimeoutError, KafkaException
from .futures import FutureRecordMetadata

# librdkafka's high-level producer (rd_kafka_producev) requires a va-list.
# We construct this list of arguments programmatically.
# See: https://github.com/confluentinc/librdkafka/blob/master/src/rdkafka.h#L3842
RD_KAFKA_V_TOPIC = 1
RD_KAFKA_V_PARTITION = 3
RD_KAFKA_V_VALUE = 4
RD_KAFKA_V_KEY = 5
RD_KAFKA_V_OPAQUE = 6
RD_KAFKA_V_MSGFLAGS = 7
RD_KAFKA_V_TIMESTAMP = 8
RD_KAFKA_V_HEADERS = 9
RD_KAFKA_V_END = 0

# For rd_kafka_produce msgflags
RD_KAFKA_MSG_F_BLOCK = 0x4


class KafkaProducer:
    """
    A Kafka client that publishes records to the Kafka cluster.
    This producer is thread-safe.
    """
    _VALID_TOPIC_REGEX = re.compile(r"^[a-zA-Z0-9._-]+$")

    def __init__(self, config: Dict[str, str]):
        """
        Initializes the Kafka Producer.

        :param config: A dictionary of librdkafka configuration properties.
                       Must include 'bootstrap.servers'.
        """
        self._config = config
        errstr_buf = ffi.new("char[]", 512)
        conf = lib.rd_kafka_conf_new()
        
                # Use ID mapping for opaque pointers
        producer_id = _cffi.get_next_opaque_id()
        _cffi._opaque_id_map[producer_id] = self
        lib.rd_kafka_conf_set_opaque(conf, ffi.cast("void *", producer_id))
        lib.rd_kafka_conf_set_log_cb(conf, _cffi_module.ffi.callback("void(*)(rd_kafka_t *, int, const char *, const char *)", _cffi_module.log_callback))
        # Set log queue length and enable connection close logging for debugging
        lib.rd_kafka_conf_set(conf, b"log.queue.length", b"100000", errstr_buf, len(errstr_buf))
        lib.rd_kafka_conf_set(conf, b"log.connection.close", b"false", errstr_buf, len(errstr_buf))
        lib.rd_kafka_conf_set_dr_msg_cb(conf, lib.delivery_report_callback)




        for key, value in config.items():
            ret = lib.rd_kafka_conf_set(conf, key.encode(), str(value).encode(), errstr_buf, len(errstr_buf))
            if ret != lib.RD_KAFKA_CONF_OK:
                err_msg = ffi.string(errstr_buf).decode('utf-8')
                lib.rd_kafka_conf_destroy(conf)
                raise KafkaConfigException(f"Failed to set configuration '{key}': {err_msg}")

        self.rk = lib.rd_kafka_new(lib.RD_KAFKA_PRODUCER, conf, errstr_buf, len(errstr_buf))
        if self.rk == ffi.NULL:
            err_msg = ffi.string(errstr_buf).decode('utf-8')
            lib.rd_kafka_conf_destroy(conf)

            raise KafkaException(f"Failed to create producer: {err_msg}")

        self._closed = threading.Event()
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()

    def _poll_loop(self):
        """Internal method for the background polling thread."""
        while not self._closed.is_set():
            try:
                lib.rd_kafka_poll(self.rk, 100) # Poll with a 100ms timeout
            except Exception as e:
                # Log error or handle gracefully
                pass # Suppress logging for now
        # print("DEBUG: _poll_loop: Exiting.")

    def send(self, topic: str, value: Optional[bytes] = None, key: Optional[bytes] = None,
             headers: Optional[List[Tuple[str, bytes]]] = None, partition: int = -1,
             timestamp_ms: int = 0) -> FutureRecordMetadata:
        """
        Asynchronously send a record to a topic.

        :param topic: The topic to publish to.
        :param value: The message payload.
        :param key: The message key.
        :param headers: A list of (key, value) tuples for record headers.
        :param partition: The partition to publish to. If -1, uses the default partitioner.
        :param timestamp_ms: The message timestamp (milliseconds since epoch).
        :return: A FutureRecordMetadata object that can be used to track the delivery status.
        :raises KafkaTimeoutError: If the message buffer is full and cannot be freed within `queue.buffering.max.ms`.
        :raises TypeError: If the topic is not a string.
        :raises ValueError: If the topic name is invalid.
        :raises AssertionError: If the producer is closed or if both key and value are None.
        """
        assert not self._closed.is_set(), 'KafkaProducer is closed'
        assert value is not None or key is not None, 'Message value and key cannot both be None'

        if not isinstance(topic, str):
            raise TypeError('topic must be a string')
        if not self._VALID_TOPIC_REGEX.match(topic) or len(topic) > 249:
            raise ValueError("topic is invalid: must be chars (a-zA-Z0-9._-), and less than 250 length")

        future = FutureRecordMetadata()
        # Use ID mapping for opaque pointers
        future_id = _cffi.get_next_opaque_id()
        _cffi._opaque_id_map[future_id] = future
        msg_opaque = ffi.cast("void *", future_id)

        # Encode topic for C
        c_topic_encoded = topic.encode('utf-8')
        c_topic_ptr = ffi.new("char[]", c_topic_encoded)

        vu_args = []

        # Topic
        vu_topic = ffi.new("rd_kafka_vu_t *")
        vu_topic.vtype = RD_KAFKA_V_TOPIC
        vu_topic.u.cstr = c_topic_ptr
        vu_args.append(vu_topic[0])

        # Partition
        vu_partition = ffi.new("rd_kafka_vu_t *")
        vu_partition.vtype = RD_KAFKA_V_PARTITION
        vu_partition.u.i32 = partition
        vu_args.append(vu_partition[0])

        # Message flags (block when queue is full)
        vu_msgflags = ffi.new("rd_kafka_vu_t *")
        vu_msgflags.vtype = RD_KAFKA_V_MSGFLAGS
        vu_msgflags.u.i = RD_KAFKA_MSG_F_BLOCK
        vu_args.append(vu_msgflags[0])

        # Opaque (future for delivery report)
        vu_opaque = ffi.new("rd_kafka_vu_t *")
        vu_opaque.vtype = RD_KAFKA_V_OPAQUE
        vu_opaque.u.ptr = msg_opaque
        vu_args.append(vu_opaque[0])

        if value is not None:
            vu_value = ffi.new("rd_kafka_vu_t *")
            vu_value.vtype = RD_KAFKA_V_VALUE
            vu_value.u.mem.ptr = ffi.from_buffer(value)
            vu_value.u.mem.size = len(value)
            vu_args.append(vu_value[0])
        
        if key is not None:
            vu_key = ffi.new("rd_kafka_vu_t *")
            vu_key.vtype = RD_KAFKA_V_KEY
            vu_key.u.mem.ptr = ffi.from_buffer(key)
            vu_key.u.mem.size = len(key)
            vu_args.append(vu_key[0])
        
        c_headers = ffi.NULL
        if headers is not None:
            c_headers = lib.rd_kafka_headers_new(len(headers))
            for h_key, h_val in headers:
                # librdkafka expects header values to be non-NULL, so pass empty bytes for None
                lib.rd_kafka_header_add(c_headers, h_key.encode(), -1, h_val if h_val is not None else b'', len(h_val) if h_val is not None else 0)
            
            vu_headers = ffi.new("rd_kafka_vu_t *")
            vu_headers.vtype = RD_KAFKA_V_HEADERS
            vu_headers.u.headers = c_headers
            vu_args.append(vu_headers[0])

        # Timestamp
        if timestamp_ms != 0:
            vu_timestamp = ffi.new("rd_kafka_vu_t *")
            vu_timestamp.vtype = RD_KAFKA_V_TIMESTAMP
            vu_timestamp.u.i64 = timestamp_ms
            vu_args.append(vu_timestamp[0])

        # End marker is not needed for rd_kafka_produceva as count is passed explicitly

        # Create C array of rd_kafka_vu_t
        vus_array = ffi.new("rd_kafka_vu_t[]", len(vu_args))
        for idx, item in enumerate(vu_args):
            vus_array[idx] = item

        err_obj = lib.rd_kafka_produceva(self.rk, vus_array, len(vu_args))
        
        if err_obj != ffi.NULL:
            err_code = lib.rd_kafka_error_code(err_obj)
            err_str_c = lib.rd_kafka_error_string(err_obj)
            err_str = ffi.string(err_str_c).decode('utf-8') if err_str_c else "未知错误"
            lib.rd_kafka_error_destroy(err_obj) # 释放错误对象

            ffi.release(c_headers) # Destroy headers immediately if produce fails
            del _cffi._opaque_id_map[future_id] # Remove future from map if not produced

            if err_code == lib.RD_KAFKA_RESP_ERR__QUEUE_FULL:
                raise KafkaTimeoutError(f"本地消息队列已满: {err_str}", error_code=err_code)
            else:
                raise KafkaProduceException(f"发送消息失败: {err_str}", error_code=err_code)

        return future

    def poll(self, timeout: float = 0) -> int:
        """
        Polls for events. Should not typically be used by applications,
        as a background polling thread is managed automatically.

        :param timeout: The maximum time to block (in seconds).
        :return: The number of events served.
        """
        return lib.rd_kafka_poll(self.rk, int(timeout * 1000))

    def flush(self, timeout: Optional[float] = None) -> int:
        """
        Wait for all outstanding produce requests to be completed.

        :param timeout: The maximum time to wait (in seconds). If None, waits indefinitely.
        :return: The number of messages still outstanding. 0 means all flushed.
        :raises KafkaException: on flush error.
        """
        timeout_ms = -1 if timeout is None else int(timeout * 1000)
        err = lib.rd_kafka_flush(self.rk, timeout_ms)
        if err != 0: # RD_KAFKA_RESP_ERR_NO_ERROR
             _check_kafka_error(err, "Failed to flush producer")
        return err

    def close(self, timeout: Optional[float] = None):
        """
        Close this producer. This method blocks until all messages are flushed.
        
        :param timeout: The maximum time to wait for flushing (in seconds).
        """
        if self._closed.is_set():
            return

        self.flush(timeout)
        self._closed.set()
        self._poll_thread.join()

        lib.rd_kafka_destroy(self.rk)

        producer_id = ffi.cast("uintptr_t", lib.rd_kafka_opaque(self.rk))
        del _cffi._opaque_id_map[producer_id]
        self.rk = ffi.NULL

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()