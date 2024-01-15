from kafka import KafkaConsumer
import json

# Kafka configuration
bootstrap_servers = 'kafka:9092'  # Replace with your Kafka bootstrap servers
group_id = 'your_consumer_group_id'  # Replace with your consumer group ID

# Create KafkaConsumer instance
consumer = KafkaConsumer('weather_topic_1',
                         group_id=group_id,
                         bootstrap_servers=bootstrap_servers)

try:
    for message in consumer:
        # Process the received message
        value = message.value.decode('utf-8')
        data = json.loads(value)
        print(f"Received message: {data}")

except KeyboardInterrupt:
    pass
finally:
    # Close the consumer
    consumer.close()
