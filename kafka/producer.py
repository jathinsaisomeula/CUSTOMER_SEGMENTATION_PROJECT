import csv
from kafka import KafkaProducer
import json
import time
import os
import uuid # Import uuid for generating unique IDs

# --- Configuration ---
# IMPORTANT: Default to 9092 to match your docker-compose.yml mapping.
# This script runs LOCALLY on your macOS, connecting to Dockerized Kafka via localhost.
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TOPIC_NAME = 'jathin_sales_data_topic' # Renamed topic for this specific dataset
CSV_FILE = 'jathin_markt_sales_data.csv' # Using your provided sales data file

def serialize_data(data):
    """
    Serializes dictionary data from CSV into simple JSON bytes.
    Adds a unique ID and performs type conversions for specific columns.
    """
    # Add a unique ID if not present or empty
    if 'id' not in data or not data['id']:
        data['id'] = str(uuid.uuid4())

    # Perform type conversions for relevant columns from jathin_markt_sales_data.csv
    for key, value in data.items():
        if value is None or value == '': # Treat empty strings/None as None for numerical conversions
            data[key] = None
        elif key == "PRICE":
            try:
                data[key] = float(value)
            except (ValueError, TypeError):
                data[key] = None # Set to None if conversion fails
        elif key == "QUANTITY_PURCHASED":
            try:
                data[key] = int(float(value)) # Convert to float first, then int to handle decimal inputs
            except (ValueError, TypeError):
                data[key] = None # Set to None if conversion fails
        # Other fields remain as strings or their original type

    return json.dumps(data).encode('utf-8')

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
                # Using 'id' for logging which is added by serialize_data
                print(f"Sent message (ID: {row.get('id', 'N/A')}, Product: {row.get('PRODUCT NAME', 'N/A')})")
                message_count += 1
                time.sleep(0.1) # Small delay to simulate real-time data flow

            producer.flush()
            print(f"\nSuccessfully sent {message_count} messages to Kafka.")

    except FileNotFoundError:
        print(f"Error: The file '{CSV_FILE}' was not found. Please ensure it's in the same directory.")
    except Exception as e:
        print(f"Error producing messages: {e}")
        print("Please ensure your Kafka container is running and accessible at the specified broker address.")
        print(f"If you are on macOS/Windows and Kafka is in Docker, confirm Docker Desktop is running and port {KAFKA_BROKER.split(':')[-1]} is correctly mapped.")
    finally:
        if producer:
            producer.close()
            print("Producer connection closed.")

if __name__ == "__main__":
    produce_messages()