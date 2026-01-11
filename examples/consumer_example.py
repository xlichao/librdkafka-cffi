# examples/consumer_example.py
import argparse
import sys
import time
import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from librdkafka_cffi import KafkaConsumer, KafkaException, KafkaConfigException, TopicPartition

def consume_messages(broker: str, group_id: str, topic: str):
    config = {
        "bootstrap.servers": broker,
        "group.id": group_id,
        "auto.offset.reset": "earliest", # For testing, we might want to start from earliest
        "enable.auto.commit": "false" # We will commit manually
    }

    consumer = None
    try:
        logger.info(f"Initializing KafkaConsumer with config: {config}")
        consumer = KafkaConsumer(config)
        logger.info(f"Subscribing to topic: {topic}")
        
        # Wrap subscription in a try-except to handle unknown topics gracefully
        try:
            consumer.subscribe([topic])
        except KafkaException as e:
            if "Unknown topic or partition" in str(e):
                logger.warning(f"Topic '{topic}' not yet available or has no partitions. Will wait for messages.")
                # Continue execution to allow the 5-second timeout logic to apply
            else:
                raise # Re-raise other Kafka exceptions during subscription

        # Try to consume a single message within a timeout
        start_time = time.time()
        timeout_seconds = 5
        message_consumed = False

        while (time.time() - start_time) < timeout_seconds:
            try:
                # Poll with a short timeout to allow checking overall timeout
                # and to give librdkafka a chance to re-subscribe if topic becomes available
                msg = consumer.poll(100) 

                if msg is None:
                    logger.debug("No message received yet, continuing to poll (or PARTITION_EOF reached).")
                    continue
                else:
                    logger.info(f"Consumed message from topic {msg.topic}, "
                                f"partition {msg.partition}, offset {msg.offset}: "
                                f"key={msg.key.decode('utf-8') if msg.key else 'None'}, "
                                f"value={msg.value.decode('utf-8') if msg.value else 'None'}")
                    
                    # Manual commit after processing
                    consumer.commit(msg)
                    logger.info(f"Committed offset {msg.offset + 1} for topic {msg.topic} partition {msg.partition}")
                    message_consumed = True
                    break # Exit after consuming one message
            except KafkaException as e:
                if "Unknown topic or partition" in str(e):
                    logger.warning(f"Topic '{topic}' not yet available or has no partitions. Continuing to poll. Error: {e}")
                    # Allow loop to continue and eventually time out
                else:
                    raise # Re-raise other Kafka exceptions to be caught by the outer block

        if not message_consumed:
            logger.info(f"No message received within {timeout_seconds} seconds.")
            # Get and print current offsets for all assigned partitions
            try:
                assigned_partitions = consumer.get_assignment() # Use the new method
                if assigned_partitions:
                    logger.info(f"Current offsets for assigned partitions:")
                    for tp in assigned_partitions:
                        # For each assigned TopicPartition, get its position
                        # The position() method expects a list of dicts or TopicPartition objects for its input
                        # Convert TopicPartition object to the expected format if necessary,
                        # but it seems it can take TopicPartition objects directly.
                        # However, for consistency with librdkafka-cffi's internal usage,
                        # we'll provide a list of dicts {topic, partition}
                        positions = consumer.position([{"topic": tp.topic, "partition": tp.partition}])
                        if positions:
                            current_offset = positions[0].offset if positions[0].offset != -1 else "No valid offset (e.g., end of log or no committed offset)"
                            logger.info(f"  Topic: {tp.topic}, Partition: {tp.partition}, Next Offset to Consume: {current_offset}")
                        else:
                            logger.info(f"  Could not determine position for Topic: {tp.topic}, Partition: {tp.partition}")
                else:
                    # This might happen if subscribe() hasn't completed assignment yet,
                    # or if the topic is empty and no partitions are assigned immediately.
                    logger.info("No partitions assigned to this consumer. This may happen if the topic is new or empty.")
            except KafkaException as e:
                logger.error(f"Error getting consumer assignment or position: {e}")
            except Exception as e:
                logger.error(f"An unexpected error occurred while getting offsets: {e}")

    except KafkaConfigException as e:
        logger.error(f"Kafka configuration error: {e}")
    except KafkaException as e:
        logger.error(f"Kafka consumer error: {e}")
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user.")
    finally:
        if consumer:
            logger.info("Closing consumer...")
            consumer.close()
            logger.info("Consumer closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Consumer Example")
    parser.add_argument("--broker", default="localhost:9092", help="Kafka broker address")
    parser.add_argument("--topic", required=True, help="Kafka topic to consume from")
    parser.add_argument("--group-id", default="my_python_consumer_group", help="Kafka consumer group ID")
    
    args = parser.parse_args()

    print(f"Starting Kafka Consumer Example. Consuming from topic '{args.topic}' with group '{args.group_id}'. Press Ctrl+C to stop.")
    consume_messages(args.broker, args.group_id, args.topic)

