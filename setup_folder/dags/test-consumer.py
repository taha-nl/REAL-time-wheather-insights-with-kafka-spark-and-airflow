"""
Streaming data consumer
"""
from datetime import datetime
from kafka import KafkaConsumer

TOPIC='weather'
KAFKA_BROKER = 'kafka:9092'


print("Connecting to Kafka")
consumer = KafkaConsumer(TOPIC, bootstrap_servers=KAFKA_BROKER)
print("Connected to Kafka")
print(f"Reading messages from the topic {TOPIC}")
for msg in consumer:

    # Extract information from kafka

    message = msg.value.decode("utf-8")

    # Transform the date format to suit the database schema
    (timestamp, vehcile_id, vehicle_type, plaza_id) = message.split(",")

    dateobj = datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')
    timestamp = dateobj.strftime("%Y-%m-%d %H:%M:%S")

    # Loading data into the database table

    print(timestamp, vehcile_id, vehicle_type, plaza_id)
