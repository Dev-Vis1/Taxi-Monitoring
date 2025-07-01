import os
import glob
import pandas as pd
from confluent_kafka import Producer
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Simplified Kafka producer configuration (fixing compatibility issues)
kafka_config = {
    'bootstrap.servers': 'kafka:29092',
    'client.id': 'taxi-data-producer',
    'acks': '1',
    'compression.type': 'snappy',
    'retries': '3'
}
producer = Producer(kafka_config)

# Optimized data loading with parallel processing
def load_taxi_data_parallel(pattern, batch_size=500, max_workers=4):
    """Load taxi data files in parallel for better performance"""
    all_files = glob.glob(pattern)
    logger.info(f"Found {len(all_files)} data files to process")
    
    # Process files in larger batches for better efficiency
    for i in range(0, len(all_files), batch_size):
        batch_files = all_files[i:i + batch_size]
        combined_data = []
        
        # Use ThreadPoolExecutor for parallel file reading
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {
                executor.submit(process_single_file, file_path): file_path 
                for file_path in batch_files
            }
            
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    file_data = future.result()
                    combined_data.extend(file_data)
                except Exception as e:
                    logger.error(f"Error processing file {file_path}: {e}")
        
        logger.info(f"Loaded batch {i//batch_size + 1} with {len(combined_data)} records")
        yield combined_data

def process_single_file(file_path):
    """Process a single file and return its data"""
    try:
        taxi_id = os.path.splitext(os.path.basename(file_path))[0]
        # Use more efficient CSV reading
        df = pd.read_csv(file_path, header=None, names=['taxi_id_file', 'timestamp', 'longitude', 'latitude'])
        df['taxi_id'] = taxi_id
        # Select only needed columns and convert to dict more efficiently
        return df[['timestamp', 'latitude', 'longitude', 'taxi_id']].to_dict(orient='records')
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        return []

# Sort data by timestamp
def sort_by_timestamp(data):
    return sorted(data, key=lambda entry: entry['timestamp'])

# Send each record to the specified Kafka topic - OPTIMIZED
def send_to_kafka_optimized(data, topic, batch_size=2000):
    """Send data to Kafka with optimized batching and error handling"""
    total_records = len(data)
    logger.info(f"Sending {total_records} records to Kafka topic '{topic}'")
    
    messages_sent = 0
    failed_messages = 0
    
    for i in range(0, total_records, batch_size):
        batch = data[i:i + batch_size]
        batch_start_time = time.time()
        
        for entry in batch:
            try:
                # Use taxi_id as partition key for better load balancing
                producer.produce(
                    topic=topic,
                    key=entry['taxi_id'],
                    value=json.dumps(entry),
                    callback=lambda err, msg, sent=messages_sent: on_delivery_optimized(err, msg, sent)
                )
                messages_sent += 1
            except BufferError:
                # If buffer is full, flush and retry
                logger.warning(f'Producer buffer full, flushing...')
                producer.flush()
                try:
                    producer.produce(
                        topic=topic,
                        key=entry['taxi_id'],
                        value=json.dumps(entry),
                        callback=lambda err, msg, sent=messages_sent: on_delivery_optimized(err, msg, sent)
                    )
                    messages_sent += 1
                except Exception as e:
                    logger.error(f"Failed to send message after retry: {e}")
                    failed_messages += 1
            except Exception as e:
                logger.error(f"Error sending message: {e}")
                failed_messages += 1
        
        # Flush after each batch to ensure delivery
        producer.flush()
        
        batch_time = time.time() - batch_start_time
        logger.info(f"Batch {i//batch_size + 1}/{(total_records-1)//batch_size + 1} sent in {batch_time:.2f}s "
                   f"({len(batch)} records)")
        
        # Small delay to prevent overwhelming the system
        time.sleep(0.1)
    
    logger.info(f"Completed sending: {messages_sent} successful, {failed_messages} failed")

# Legacy function for backward compatibility
def send_to_kafka(data, topic, batch_size=1000):
    return send_to_kafka_optimized(data, topic, batch_size)

# Optimized callback function
def on_delivery_optimized(err, msg, message_count):
    """Optimized delivery callback with reduced logging"""
    if err:
        logger.error(f'Message delivery failed: {err}')
    elif message_count % 1000 == 0:  # Log every 1000th message
        logger.info(f'Delivered {message_count} messages to {msg.topic()}')

# Callback for message delivery status - LEGACY
def on_delivery(err, msg):
    if err:
        print(f'Delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [Partition {msg.partition()}] at offset {msg.offset()}')


def main():
    input_pattern = 'data/*.txt'
    kafka_topic = 'taxi-locations'
    
    logger.info("Starting optimized taxi data producer")
    start_time = time.time()
    
    total_records = 0
    for data_batch in load_taxi_data_parallel(input_pattern):
        sorted_data = sort_by_timestamp(data_batch)
        send_to_kafka_optimized(sorted_data, kafka_topic)
        total_records += len(sorted_data)
    
    end_time = time.time()
    logger.info(f"Producer completed: {total_records} records sent in {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
