from kafka import KafkaConsumer
import json
import os # Import os to use os.getenv
import time

# --- Configuration ---
# IMPORTANT: This script runs LOCALLY on your macOS.
# Based on your 'docker compose ps' output, Kafka is accessible on localhost:9092.
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092') # <--- CORRECTED PORT TO 9092
TOPIC_NAME = 'jathin_sales_data_topic' # <--- CORRECTED TOPIC NAME to match producer
GROUP_ID = 'jathin_sales_consumer_group_new_test'

def deserialize_data(data_bytes): # Renamed 'data' to 'data_bytes' for clarity as it's bytes
    """Deserialize JSON bytes from Kafka message."""
    # Decode the bytes received from Kafka back into a UTF-8 string.
    decoded_string = data_bytes.decode('utf-8')
    # Parse the JSON string into a Python dictionary.
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
            group_id=GROUP_ID, # Assign a consumer group ID
            value_deserializer=deserialize_data,
            # request_timeout_ms=30000, # Uncomment if you face connection timeouts
            # api_version=(0, 10, 1) # Uncomment if you encounter API version mismatch errors
        )
        print(f"Consumer connected to Kafka broker: {KAFKA_BROKER}")
        print(f"Subscribed to topic: '{TOPIC_NAME}' in group '{GROUP_ID}'. Waiting for messages... (Press Ctrl+C to stop)")

        # Process messages
        for message in consumer:
            sales_record = message.value # 'message.value' is now a Python dictionary after deserialization
            print(f"--- Received Sales Record (Offset: {message.offset}, Partition: {message.partition}) ---")
            # Print specific fields for better readability of structured data
            print(f"  ID: {sales_record.get('id', 'N/A')}") # Include the UUID
            print(f"  Product Type: {sales_record.get('PRODUCT TYPE', 'N/A')}")
            print(f"  Product ID: {sales_record.get('PRODUCT ID', 'N/A')}")
            print(f"  Product Name: {sales_record.get('PRODUCT NAME', 'N/A')}")
            print(f"  Price: {sales_record.get('PRICE', 'N/A')} (Type: {type(sales_record.get('PRICE'))})")
            print(f"  Quantity: {sales_record.get('QUANTITY_PURCHASED', 'N/A')} (Type: {type(sales_record.get('QUANTITY_PURCHASED'))})")
            print(f"  Date of Sale: {sales_record.get('DATE OF SALE', 'N/A')}")
            print("-" * 40)
            time.sleep(0.05) # Small delay to simulate processing

    except KeyboardInterrupt:
        print("\nConsumer gracefully stopped by user.")
    except Exception as e:
        print(f"Error consuming messages: {e}")
        print("Please ensure your Kafka container is running, the topic exists, and the broker address is correct.")
        # Corrected port in the help message
        print(f"If you are on macOS/Windows and Kafka is in Docker, confirm Docker Desktop is running and port {KAFKA_BROKER.split(':')[-1]} is correctly mapped.")
    finally:
        if consumer:
            consumer.close()
            print("Consumer connection closed.")

if __name__ == "__main__":
    consume_messages()