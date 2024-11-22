from ._librdkafka_cffi import ffi,lib
from cffi import FFI
import logging
from dataclasses import dataclass

logger = logging.getLogger()

RD_KAFKA_PARTITION_UA=-1

@ffi.def_extern()
def log_callback(rk,level:int,fac,buf):
    fac = ffi.string(fac)
    buf = ffi.string(buf)
    print(f"{fac} : {buf}")

@dataclass
class KafkaMessage:
    has_error:bool = False
    error_msg:str = None
    key:str = None
    value:str = None
    topic:str = None
    partition:int = -1
    offset:int = -1

    @staticmethod
    def from_cdata(rkm):
        # 从 cdata 读取，未实现时间戳
        if rkm == ffi.NULL:
            return None
        msg = KafkaMessage()
        # 从 rkm 中读取
        if rkm.err != 0:
            error_str = lib.rd_kafka_message_errstr(rkm)
            msg.has_error = True
            msg.error_msg = ffi.string(error_str).decode()
            lib.rd_kafka_message_destroy(rkm)
            return msg
        # 这里应该可以缓存 rkt 到 topic 名的映射，或者外部一次性读取一堆消息
        topic_name = lib.rd_kafka_topic_name(rkm.rkt)
        msg.topic = ffi.string(topic_name).decode()
        msg.partition = rkm.partition
        msg.offset = rkm.offset
        # 处理 payload
        if rkm.payload != ffi.NULL:
            # rkm.payload 是个  void * 需要强制转换
            payload = ffi.cast("const char *",rkm.payload)
            value:bytes = ffi.string(payload,rkm.len)
            msg.value = value.decode()
        if rkm.key != ffi.NULL:
            # rkm.payload 是个  void * 需要强制转换
            payload = ffi.cast("const char *",rkm.key)
            key:bytes = ffi.string(payload,rkm.key_len)
            msg.key = key.decode()
        lib.rd_kafka_message_destroy(rkm)
        return msg

    def error(self):
        if self.has_error:
            return self.error_msg
        else:
            return None


class KafkaConsumer:
    # 基于 https://github.com/confluentinc/librdkafka/blob/master/examples/consumer.c
    def __init__(self,config_dict:dict[str,str]) -> None:
        version_str = lib.rd_kafka_version_str()
        logger.info("Found librdkafka %s",ffi.string(version_str).decode())
        conf = lib.rd_kafka_conf_new()
        lib.rd_kafka_conf_set_log_cb(conf, lib.log_callback)
        self.error_len = 1024
        self.error_str = ffi.new(f"char [{self.error_len}]")
        for key,value in config_dict.items():
            ret = lib.rd_kafka_conf_set(conf,key.encode(),value.encode(),self.error_str,self.error_len)
            if ret != 0:
                self._show_error()
                lib.rd_kafka_conf_destroy(conf)
                raise ValueError
        self.rk = lib.rd_kafka_new(lib.RD_KAFKA_CONSUMER,conf,self.error_str,self.error_len)
        if self.rk == ffi.NULL:
            self._show_error()
            raise ValueError
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
        if err != lib.RD_KAFKA_RESP_ERR_NO_ERROR:
            self._show_error()
            lib.rd_kafka_topic_partition_list_destroy(subscription)
            lib.rd_kafka_destroy(self.rk)
            raise ValueError
        print(f"Subscribed to {len(topics)} topic(s)")
        lib.rd_kafka_topic_partition_list_destroy(subscription)
    
    def poll(self,timeout:float):
        timeout_ms = int(timeout * 1000)
        if self.rk == ffi.NULL:
            raise RuntimeError
        rkm = lib.rd_kafka_consumer_poll(self.rk, timeout_ms)
        msg = KafkaMessage.from_cdata(rkm)
        return msg

    def close(self):
        lib.rd_kafka_consumer_close(self.rk)
        lib.rd_kafka_destroy(self.rk)
        self.rk = ffi.NULL

    def _show_error(self):
        error_meg:bytes = ffi.string(self.error_str,self.error_len)
        error_meg = error_meg.decode()
        logger.error(f'librdkakfa report {error_meg}')
        


