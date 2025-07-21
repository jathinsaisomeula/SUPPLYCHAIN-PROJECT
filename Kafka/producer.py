# producer.py (Corrected for simple JSON output, compatible with spark_processor.py)
import csv
from kafka import KafkaProducer
import json
import time
import os
import uuid # Import uuid for generating unique IDs

# --- Configuration ---
# IMPORTANT: Default to 9092 to match your docker-compose.yml mapping.
# This script runs LOCALLY on your macOS, connecting to Dockerized Kafka via localhost.
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092') # <--- CORRECTED PORT
TOPIC_NAME = 'supply_chain_data'
CSV_FILE = 'supply_chain_data.csv' # Ensure this file exists in the same directory

def serialize_data(data):
    """
    Serializes dictionary data from CSV into simple JSON bytes.
    This is the format expected by our current spark_processor.py.
    """
    # Add a unique ID if not present in the CSV row
    if 'id' not in data or not data['id']:
        data['id'] = str(uuid.uuid4())

    # Perform type conversions as needed (simplified from your original schema logic)
    # This is a basic example; for robust type handling, consider a dedicated parsing function
    # or ensure your CSV reader handles types correctly.
    for key, value in data.items():
        if value == '': # Treat empty strings as None for numerical conversions
            data[key] = None
        elif key in ["Stock levels", "Lead time", "Order quantities", "Production volumes", "Safety stock levels"]:
            try:
                data[key] = int(float(value)) if value is not None else None
            except (ValueError, TypeError):
                data[key] = None # Set to None if conversion fails
        elif key in ["Shipping costs", "Manufacturing costs", "Defect rates"]:
            try:
                data[key] = float(value) if value is not None else None
            except (ValueError, TypeError):
                data[key] = None # Set to None if conversion fails
        # All other fields remain as strings or their original type

    return json.dumps(data).encode('utf-8') # <--- Simple JSON without schema wrapper

def produce_messages():
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=serialize_data,
            # request_timeout_ms=30000, # Uncomment if you face connection timeouts
            # api_version=(0, 10, 1) # Uncomment if you encounter API version mismatch errors
        )
        print(f"Producer connected to Kafka broker: {KAFKA_BROKER}")

        with open(CSV_FILE, mode='r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            print(f"Reading data from '{CSV_FILE}' and sending to topic '{TOPIC_NAME}'...")
            message_count = 0
            for row in csv_reader:
                producer.send(TOPIC_NAME, row) # 'row' will be processed by serialize_data
                print(f"Sent message (ID: {row.get('id', 'N/A')}, Product: {row.get('Product type', 'N/A')})")
                message_count += 1
                time.sleep(0.1) # Small delay to simulate real-time data flow

            producer.flush()
            print(f"\nSuccessfully sent {message_count} messages to Kafka.")

    except Exception as e:
        print(f"Error producing messages: {e}")
        print("Please ensure your Kafka container is running and accessible at the specified broker address.")
        # Corrected help message to reflect the actual port in use
        print(f"If you are on macOS/Windows and Kafka is in Docker, confirm Docker Desktop is running and port {KAFKA_BROKER.split(':')[-1]} is correctly mapped.")
    finally:
        if producer:
            producer.close()
            print("Producer connection closed.")

if __name__ == "__main__":
    produce_messages()