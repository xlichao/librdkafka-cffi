
from dataclasses import dataclass
from ._cffi import ffi, lib, _check_kafka_error, NO_ERROR
from .errors import KafkaConfigException, KafkaException
import time

RD_KAFKA_PARTITION_UA=-1



@dataclass
class Message:
    has_error:bool = False
    error_msg:str = None
    key:str = None
    value:str = None
    topic:str = None
    partition:int = -1
    offset:int = -1

    @staticmethod
    def from_cdata(rkm):
        # Assumes rkm is not FFI.NULL and rkm.err == 0 (error handled by poll)
        msg = Message()
        topic_name = lib.rd_kafka_topic_name(rkm.rkt)
        msg.topic = ffi.string(topic_name).decode()
        msg.partition = rkm.partition
        msg.offset = rkm.offset
        # 处理 payload
        if rkm.payload != ffi.NULL:
            payload = ffi.cast("const char *",rkm.payload)
            value:bytes = ffi.string(payload,rkm.len)
            msg.value = value
        if rkm.key != ffi.NULL:
            payload = ffi.cast("const char *",rkm.key)
            key:bytes = ffi.string(payload,rkm.key_len)
            msg.key = key
        lib.rd_kafka_message_destroy(rkm)
        return msg

    def error(self):
        if self.has_error:
            return self.error_msg
        else:
            return None

@dataclass
class TopicPartition:
    topic: str
    partition: int
    offset: int = -1 # -1 can indicate an invalid or uninitialized offset


class KafkaConsumer:
    # 基于 https://github.com/confluentinc/librdkafka/blob/master/examples/consumer.c
    def __init__(self,config_dict:dict[str,str]) -> None:
        version_str = lib.rd_kafka_version_str()
        # logger.info("Found librdkafka %s",ffi.string(version_str).decode()) # Handled by global log_callback
        conf = lib.rd_kafka_conf_new()
        lib.rd_kafka_conf_set_log_cb(conf, lib.log_callback) # Use the global log_callback
        # No need for self.error_len and self.error_str as error handling is centralized
        
        # Store a reference to this Python object in the opaque pointer
        self._c_opaque = ffi.new_handle(self)
        lib.rd_kafka_conf_set_opaque(conf, self._c_opaque)
        lib.rd_kafka_conf_set_rebalance_cb(conf, lib.rebalance_callback)

        errstr_buf = ffi.new("char[]", 512) # Buffer for error messages from librdkafka
        
        for key,value in config_dict.items():
            ret = lib.rd_kafka_conf_set(conf,key.encode(),value.encode(),errstr_buf, len(errstr_buf))
            if ret != NO_ERROR:
                err_msg = ffi.string(errstr_buf).decode('utf-8')
                lib.rd_kafka_conf_destroy(conf)
                raise KafkaConfigException(f"Failed to set configuration property '{key}'='{value}': {err_msg}")
        
        self.rk = lib.rd_kafka_new(lib.RD_KAFKA_CONSUMER,conf,errstr_buf, len(errstr_buf))
        if self.rk == ffi.NULL:
            err_msg = ffi.string(errstr_buf).decode('utf-8')
            lib.rd_kafka_conf_destroy(conf)
            ffi.release(self._c_opaque) # Release the handle if creation fails
            raise KafkaException(f"Failed to create Kafka consumer: {err_msg}")
        
        lib.rd_kafka_poll_set_consumer(self.rk)
    
    def subscribe(self,topics:list[str]):
        if not isinstance(topics,list):
            raise ValueError
        if self.rk == ffi.NULL:
            raise RuntimeError
        subscription = lib.rd_kafka_topic_partition_list_new(len(topics))
        for topic in topics:
            lib.rd_kafka_topic_partition_list_add(subscription, topic.encode(),
                                                  RD_KAFKA_PARTITION_UA)
        err = lib.rd_kafka_subscribe(self.rk, subscription)
        _check_kafka_error(err, "Failed to subscribe to topics") # Use the unified error checker
        # print(f"Subscribed to {len(topics)} topic(s)") # Removed as per logging centralization
        lib.rd_kafka_topic_partition_list_destroy(subscription)
    
    def poll(self, timeout:float):
        timeout_ms = int(timeout * 1000)
        if self.rk == ffi.NULL:
            raise RuntimeError("Consumer is not initialized.")
        rkm = lib.rd_kafka_consumer_poll(self.rk, timeout_ms)
        
        if rkm == ffi.NULL: # Timeout, no message
            return None
        
        if rkm.err != 0:
            error_code = rkm.err
            error_str = ffi.string(lib.rd_kafka_err2str(error_code)).decode('utf-8')
            lib.rd_kafka_message_destroy(rkm) # Destroy the C message
            
            if error_code == lib.RD_KAFKA_RESP_ERR__PARTITION_EOF:
                time.sleep(timeout)
                return None # As requested, return None for EOF
            else:
                # For other errors, raise a KafkaException
                raise KafkaException(f"Consumer error: {error_str} (Code: {error_code})")
        
        # If no error, proceed to create Message object
        msg = Message.from_cdata(rkm)
        return msg

    def close(self):
        lib.rd_kafka_consumer_close(self.rk)
        # Release the Python object's handle
        # ffi.release(self._c_opaque) # Removed for consistency and to test hypothesis
        lib.rd_kafka_destroy(self.rk)
        self.rk = ffi.NULL

    def _rebalance_assign(self, partitions):
        """Internal method to assign partitions during rebalance."""
        _check_kafka_error(lib.rd_kafka_assign(self.rk, partitions), "Failed to assign partitions")
        # You might want to log this or notify the user
        
    def _rebalance_revoke(self, partitions):
        """Internal method to revoke partitions during rebalance."""
        _check_kafka_error(lib.rd_kafka_assign(self.rk, ffi.NULL), "Failed to revoke partitions")
        # You might want to log this or notify the user

    def commit(self, message=None, async_commit=False):
        """
        Commit offsets.
        If message is provided, commits the offset of that message.
        Otherwise, commits the current assigned offsets.
        """
        if message:
            tps = lib.rd_kafka_topic_partition_list_new(1)
            lib.rd_kafka_topic_partition_list_add(tps, message.topic.encode(), message.partition)
            tps.elems[0].offset = message.offset + 1 # Commit the next offset
        else:
            tps = ffi.NULL # Commit current assigned offsets

        flags = 0 if not async_commit else 1 # 0 for synchronous commit, 1 for asynchronous commit
        err = lib.rd_kafka_commit(self.rk, tps, flags)
        _check_kafka_error(err, "Failed to commit offsets")

        if tps != ffi.NULL:
            lib.rd_kafka_topic_partition_list_destroy(tps)

    def assign(self, topic_partitions: list[tuple[str, int, int]]):
        """
        Manually assign partitions to the consumer.
        topic_partitions is a list of (topic, partition, offset) tuples.
        Offset can be RD_KAFKA_OFFSET_BEGINNING, RD_KAFKA_OFFSET_END, etc.
        """
        tps = lib.rd_kafka_topic_partition_list_new(len(topic_partitions))
        for topic, partition, offset in topic_partitions:
            lib.rd_kafka_topic_partition_list_add(tps, topic.encode(), partition)
            tps.elems[tps.cnt - 1].offset = offset
        
        _check_kafka_error(lib.rd_kafka_assign(self.rk, tps), "Failed to assign partitions manually")
        lib.rd_kafka_topic_partition_list_destroy(tps)

    def get_assignment(self) -> list[TopicPartition]:
        """
        Get the current partition assignment for this consumer.
        Returns a list of TopicPartition objects.
        """
        if self.rk == ffi.NULL:
            raise RuntimeError("Consumer is not initialized.")

        tps_list_ptr = ffi.new("rd_kafka_topic_partition_list_t **")
        err = lib.rd_kafka_assignment(self.rk, tps_list_ptr)
        _check_kafka_error(err, "Failed to get current assignment")

        assignment = []
        if tps_list_ptr[0] != ffi.NULL:
            tps = tps_list_ptr[0]
            for i in range(tps.cnt):
                tp_cdata = tps.elems[i]
                topic_name = ffi.string(tp_cdata.topic).decode('utf-8')
                assignment.append(TopicPartition(
                    topic=topic_name,
                    partition=tp_cdata.partition,
                    offset=tp_cdata.offset # This offset might not be relevant here, but keeping for consistency
                ))
            lib.rd_kafka_topic_partition_list_destroy(tps)
        
        return assignment




        


