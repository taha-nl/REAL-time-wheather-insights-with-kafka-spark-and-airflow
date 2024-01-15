from kafka import KafkaConsumer
import json

# Define Kafka consumer
consumer = KafkaConsumer('weather_topic_1', bootstrap_servers='kafka:9092', group_id='weather_consumer_group')

# Consume messages
for message in consumer:
    try:
        # Decode the message value and load JSON data
        data = json.loads(message.value.decode('utf-8'))
        
        # Process the data as needed
        print("Received data:", data)
        
        # Example: Accessing temperature and date
        temperature = data['weather']['temperature']
        date = data['date']
        print(f"Temperature: {temperature}, Date: {date}")

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")

    except Exception as e:
        print(f"Error processing message: {e}")
