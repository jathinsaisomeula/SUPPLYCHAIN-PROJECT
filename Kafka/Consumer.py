# consumer.py (Corrected for running on macOS host)
from kafka import KafkaConsumer
import json
import os # Import os to use os.getenv if you want

# --- Configuration ---
# IMPORTANT: This script runs LOCALLY on your macOS.
# 'localhost:9092' is the correct address to connect to your Dockerized Kafka
# because your docker-compose.yml maps Kafka's internal 9092 to your host's 9092.
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092') # <--- CORRECTED PORT
TOPIC_NAME = 'supply_chain_data'

def deserialize_data(data):
    """Deserialize JSON bytes from Kafka message."""
    # Decode the bytes received from Kafka back into a UTF-8 string.
    decoded_string = data.decode('utf-8')
    # Parse the JSON string into a Python dictionary or list.
    return json.loads(decoded_string)

def consume_messages():
    consumer = None
    try:
        # Initialize Kafka Consumer
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest', # Start reading from the beginning of the topic (useful for testing)
            enable_auto_commit=True,      # Automatically commit offsets
            group_id='my-supply-chain-group', # Assign a consumer group ID
            value_deserializer=deserialize_data,
            # request_timeout_ms=30000, # Uncomment if you face connection timeouts
            # api_version=(0, 10, 1) # Uncomment if you encounter API version mismatch errors
        )
        print(f"Consumer connected to Kafka broker: {KAFKA_BROKER}")
        print(f"Subscribed to topic: '{TOPIC_NAME}'. Waiting for messages... (Press Ctrl+C to stop)")

        # Process messages
        for message in consumer:
            print(f"Received message (Offset: {message.offset}, Partition: {message.partition}):")
            print(f"  Value: {message.value}") # 'message.value' is now a Python dictionary
            print("-" * 20)

    except KeyboardInterrupt:
        print("\nConsumer gracefully stopped by user.")
    except Exception as e:
        print(f"Error consuming messages: {e}")
        print("Please ensure your Kafka container is running and accessible at the specified broker address.")
        # Corrected port in the help message
        print(f"If you are on macOS/Windows and Kafka is in Docker, confirm Docker Desktop is running and port {KAFKA_BROKER.split(':')[-1]} is correctly mapped.")
    finally:
        if consumer:
            consumer.close()
            print("Consumer connection closed.")

if __name__ == "__main__":
    consume_messages()