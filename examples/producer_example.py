# examples/producer_example.py
import sys
import logging
import argparse

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from librdkafka_cffi import KafkaProducer
from librdkafka_cffi.errors import KafkaException, KafkaTimeoutError

def main():
    """
    一个命令行的 Kafka 生产工具，用于发送单条消息并等待结果。
    """
    parser = argparse.ArgumentParser(description="简单的 Kafka 生产者命令行工具")
    parser.add_argument(
        '--bootstrap-servers',
        default='127.0.0.1:9092',
        help="Kafka brokers 的地址，格式为 'host:port,host2:port2'。 (默认: 'localhost:9092')"
    )
    parser.add_argument(
        '--topic',
        required=True,
        help="需要发送消息的目标 Topic。"
    )
    parser.add_argument(
        '--key',
        default=None,
        help="消息的 key (可选)。"
    )
    parser.add_argument(
        '--value',
        required=True,
        help="消息的 value。"
    )
    parser.add_argument(
        '--header',
        action='append',
        help="消息的 header，格式为 'key=value'。可以多次指定 (例如: --header X-Request-ID=123)。"
    )

    args = parser.parse_args()

    # 1. 准备生产者配置
    # 更多配置选项请参考:
    # https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    config = {
        "bootstrap.servers": args.bootstrap_servers,
        "acks": "all",  # 确保消息被所有 in-sync replicas 确认
        "queue.buffering.max.ms": 500, # send() 调用在队列满时最多阻塞 500ms
        "broker.address.family": "v4",
    }

    # 准备 Headers
    headers = []
    if args.header:
        for header_str in args.header:
            if '=' not in header_str:
                logger.error(f"Header 格式错误: '{header_str}'，应为 'key=value'。")
                sys.exit(1)
            key, value = header_str.split('=', 1)
            headers.append((key, value.encode('utf-8')))

    # 将 key 和 value 编码为 bytes
    key_bytes = args.key.encode('utf-8') if args.key else None
    value_bytes = args.value.encode('utf-8')

    # 2. 创建 KafkaProducer 实例
    # 使用 'with' 语句可以确保在代码块结束时自动调用 producer.close()
    logger.info(f"正在初始化 KafkaProducer, 配置: {config}")
    try:
        with KafkaProducer(config) as producer:
            logger.info(f"准备发送消息到 Topic '{args.topic}'...")
            logger.info(f"  - Key: {args.key}")
            logger.info(f"  - Value: {args.value}")
            if headers:
                logger.info(f"  - Headers: {[(h[0], h[1].decode()) for h in headers]}")

            # 3. 发送消息
            # producer.send() 是一个异步方法，它会立即返回一个 FutureRecordMetadata 对象。
            # 这个 Future 对象可以用来查询消息的最终发送状态。
            # 由于我们在 send() 方法内部使用了 RD_KAFKA_MSG_F_BLOCK 标志,
            # 如果 librdkafka 的内部队列已满, 这个调用会阻塞直到有空间可用或超时。
            future = producer.send(
                args.topic,
                value=value_bytes,
                key=key_bytes,
                headers=headers
            )
            logger.info("消息已提交到内部队列，正在等待发送回执...")

            # 4. 等待结果
            # 调用 future.result() 会阻塞当前线程，直到消息被成功发送并收到 broker 的确认，
            # 或者发送失败。
            # 如果成功，它会返回一个 RecordMetadata 对象；如果失败，则会抛出异常。
            result = future.result(timeout=10)  # 设置 10 秒的等待超时

            logger.info("消息发送成功！🎉")
            logger.info(f"  - Topic: {result.topic}")
            logger.info(f"  - Partition: {result.partition}")
            logger.info(f"  - Offset: {result.offset}")

    except KafkaTimeoutError as e:
        logger.error(f"发送消息超时: {e}")
        logger.error("这可能是因为 Kafka broker 连接不上，或者内部队列已满且长时间无法清空。")
        sys.exit(1)
    except KafkaException as e:
        logger.error(f"发送消息时发生 Kafka 错误: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"发生了预料之外的错误: {e}",exc_info=e)
        sys.exit(1)

    logger.info("演示完成。")

if __name__ == "__main__":
    main()