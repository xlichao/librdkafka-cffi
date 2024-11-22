from librdkafka_cffi import KafkaConsumer,KafkaMessage
import logging

logger = logging.getLogger()

if __name__ == "__main__":
    # 测试流程来源于 https://github.com/confluentinc/examples/blob/master/clients/cloud/python/consumer.py
    config = {
        # User-specific properties that you must set
        'bootstrap.servers': 'kafka:9092',
        'group.id':          'lalala',
        'auto.offset.reset': 'earliest',
        'log_level' : '0',
    }
    consumer = KafkaConsumer(config)
    consumer.subscribe(["lalala"])

    count = 0
    try:
        while True:
            print(f"{count} Polling...",end="")
            msg = consumer.poll(1.0)
            print(f"{count} Polled  \r",end="")
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                count += 1
            else:
                # Extract the (optional) key and value, and print.
                print(msg)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error("polling 异常：",exc_info=e)
    finally:
        # Leave group and commit final offsets
        consumer.close()
