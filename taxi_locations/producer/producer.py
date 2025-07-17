import os
import glob
import pandas as pd
from confluent_kafka import Producer
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
from datetime import datetime, timedelta


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

kafka_config = {
    'bootstrap.servers': 'kafka:29092',
    'client.id': 'taxi-data-producer',
    'acks': '0', 
    'compression.type': 'lz4',  
    'batch.size': 500000, 
    'batch.num.messages': 50000,  
    'linger.ms': 10,  
    'queue.buffering.max.messages': 1000000,  
    'queue.buffering.max.ms': 50, 
    'message.max.bytes': 10000000,
    'socket.keepalive.enable': True,
    'socket.send.buffer.bytes': 131072,  
    'socket.receive.buffer.bytes': 65536,  
    'enable.idempotence': False,  
    'max.in.flight.requests.per.connection': 10  
}
producer = Producer(kafka_config)

# Data loading with parallel processing
def load_taxi_data_parallel(pattern, batch_size=500, max_workers=4):
    """Load taxi data files in parallel for better performance"""
    all_files = glob.glob(pattern)
    logger.info(f"Found {len(all_files)} data files to process")
    
    
    for i in range(0, len(all_files), batch_size):
        batch_files = all_files[i:i + batch_size]
        combined_data = []
        
        #  ThreadPoolExecutor for parallel file reading
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
    """Ultra-fast file processing with minimal overhead"""
    try:
        taxi_id = os.path.splitext(os.path.basename(file_path))[0]
        
      
        try:
            df = pd.read_csv(file_path, 
                           header=None,
                           names=['taxi_id_file', 'timestamp', 'longitude', 'latitude'],
                           dtype={'taxi_id_file': 'str', 'longitude': 'float64', 'latitude': 'float64'})
        except Exception:
            
            chunks = []
            for chunk in pd.read_csv(file_path, 
                                   header=None,
                                   names=['taxi_id_file', 'timestamp', 'longitude', 'latitude'],
                                   chunksize=10000): 
                chunks.append(chunk)
            
            if not chunks:
                return []
            df = pd.concat(chunks, ignore_index=True)
        
        initial_count = len(df)
        
        df = df[(df['latitude'] != 0) & (df['longitude'] != 0) & 
                df['latitude'].notna() & df['longitude'].notna()]
        
        df = df.drop_duplicates(subset=['timestamp', 'longitude', 'latitude'], keep='first')
        
        if len(df) == 0:
            return []
        
        df['taxi_id'] = taxi_id
        
        return df[['timestamp', 'latitude', 'longitude', 'taxi_id']].to_dict(orient='records')
        
    except Exception as e:
        logger.debug(f"Error processing {file_path}: {e}")  
        return []

def sort_by_timestamp(data):
    return sorted(data, key=lambda entry: entry['timestamp'])

# REAL-TIME TEMPORAL STREAMING
def send_to_kafka_temporal_streaming(data, topic, speed_multiplier=120):
    logger.info(f"Starting temporal streaming for {len(data)} records with {speed_multiplier}x speed")
    
    if not data:
        return
    
    datetime_format = "%Y-%m-%d %H:%M:%S"
    
    parsed_data = []
    for entry in data:
        try:
            dt = datetime.strptime(entry['timestamp'], datetime_format)
            parsed_data.append((dt, entry))
        except ValueError as e:
            logger.warning(f"Invalid timestamp format: {entry['timestamp']} - skipping")
            continue
    
    if not parsed_data:
        logger.error("No valid timestamps found in data")
        return
    
    # Sort by timestamp globally to ensure chronological order
    parsed_data.sort(key=lambda x: x[0])
    logger.info(f"Sorted {len(parsed_data)} records chronologically")
    
    # Calculate the time range and streaming parameters
    start_time = parsed_data[0][0]
    end_time = parsed_data[-1][0]
    total_duration = (end_time - start_time).total_seconds()
    
    logger.info(f"Data time range: {start_time} to {end_time} ({total_duration:.0f} seconds)")
    logger.info(f"Streaming duration: {total_duration/speed_multiplier:.1f} seconds")
    
    import time as time_module
    messages_sent = 0
    streaming_start = time_module.time()
    last_log_time = streaming_start
    
    # Track last watermark sent time
    last_watermark_time = streaming_start
    
    for i, (dt, entry) in enumerate(parsed_data):
        time_since_start = (dt - start_time).total_seconds()
        target_real_time = streaming_start + (time_since_start / speed_multiplier)
        current_real_time = time_module.time()
        
        # Wait if needed to maintain proper timing
        if target_real_time > current_real_time:
            sleep_time = target_real_time - current_real_time
            if sleep_time > 0.01:  
                time_module.sleep(sleep_time)
        
        try:
            clean_entry = {
                'taxi_id': entry['taxi_id'],
                'timestamp': entry['timestamp'],
                'latitude': entry['latitude'],
                'longitude': entry['longitude']
            }
            
            producer.produce(
                topic=topic,
                key=entry['taxi_id'],
                value=json.dumps(clean_entry),
                callback=lambda err, msg, seq=messages_sent: delivery_callback_minimal(err, msg, seq)
            )
            messages_sent += 1
            
            # Send watermark
            if messages_sent % 100 == 0 or (time_module.time() - last_watermark_time) > 5:
                watermark_ts = int(dt.timestamp() * 1000) 
                producer.produce(
                    topic=topic,
                    key="WATERMARK",
                    value=json.dumps({"watermark": watermark_ts})
                )
                last_watermark_time = time_module.time()
                logger.debug(f"Sent watermark: {watermark_ts}")
            
            # Flush regularly for smooth streaming
            if messages_sent % 100 == 0:
                producer.flush()
                
        except Exception as e:
            logger.error(f"Failed to send message {messages_sent}: {e}")
        
        # Progress logging
        if time_module.time() - last_log_time > 5:  
            progress = (i + 1) / len(parsed_data) * 100
            logger.info(f"Streaming progress: {progress:.1f}% ({messages_sent} messages sent)")
            last_log_time = time_module.time()
    
    final_watermark = int(end_time.timestamp() * 1000)
    producer.produce(
        topic=topic,
        key="WATERMARK",
        value=json.dumps({"watermark": final_watermark})
    )
    logger.info(f"Sent final watermark: {final_watermark}")
    
    producer.flush()
    total_time = time_module.time() - streaming_start
    logger.info(f"Temporal streaming completed: {messages_sent} messages sent in {total_time:.2f}s")
    logger.info(f"Average rate: {messages_sent/total_time:.1f} messages/second")

def delivery_callback_minimal(err, msg, sequence):
    """Minimal callback for better performance"""
    if err:
        logger.error(f'Delivery failed for message {sequence}: {err}')
    elif sequence % 200 == 0:  
        logger.info(f'Smooth streaming: delivered message {sequence}')

# Send each record to the specified Kafka topic 
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
        
        producer.flush()
        
        batch_time = time.time() - batch_start_time
        logger.info(f"Batch {i//batch_size + 1}/{(total_records-1)//batch_size + 1} sent in {batch_time:.2f}s "
                   f"({len(batch)} records)")
        
        time.sleep(0.1)
    
    logger.info(f"Completed sending: {messages_sent} successful, {failed_messages} failed")

#  backward compatibility
def send_to_kafka(data, topic, batch_size=1000):
    return send_to_kafka_optimized(data, topic, batch_size)

# callback 
def on_delivery_optimized(err, msg, message_count):
    """Optimized delivery callback with reduced logging"""
    if err:
        logger.error(f'Message delivery failed: {err}')
    elif message_count % 1000 == 0:  
        logger.info(f'Delivered {message_count} messages to {msg.topic()}')

# Callback Status 
def on_delivery(err, msg):
    if err:
        print(f'Delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [Partition {msg.partition()}] at offset {msg.offset()}')

def send_to_kafka_every_2_seconds(data, topic, update_interval=2.0):
    """Send taxi updates every 2 seconds for smooth real-time visualization with GLOBAL chronological order"""
    if not data:
        logger.warning("No data to send")
        return
    
    logger.info(f"Starting real-time streaming with {update_interval}s updates")
    
    parsed_data = []
    for entry in data:
        try:
            dt = datetime.strptime(entry['timestamp'], '%Y-%m-%d %H:%M:%S')
            parsed_data.append((dt, entry))
        except ValueError as e:
            logger.warning(f"Invalid timestamp format: {entry.get('timestamp')} - {e}")
            continue
    
    if not parsed_data:
        logger.error("No valid data to process")
        return
        
    
    parsed_data.sort(key=lambda x: x[0])
    
    start_time = parsed_data[0][0]
    end_time = parsed_data[-1][0]
    total_duration = (end_time - start_time).total_seconds()
    
    logger.info(f"Data time range: {start_time} to {end_time} ({total_duration:.0f} seconds)")
    logger.info(f"Will stream {len(parsed_data)} records in chronological order with {update_interval}s interval updates")
    
    
    import time as time_module
    messages_sent = 0
    streaming_start = time_module.time()
    
    # Keep track of last timestamp sent to ensure- never go backwards
    last_timestamp = None
    
    
    data_index = 0
    
    while data_index < len(parsed_data):
        batch_start = time_module.time()
        batch_count = 0
        
        current_time_window = parsed_data[data_index][0]
        window_end = current_time_window + timedelta(seconds=30)
        
        while data_index < len(parsed_data) and parsed_data[data_index][0] <= window_end:
            dt, entry = parsed_data[data_index]
            
            if last_timestamp is None or dt >= last_timestamp:
                try:
                    clean_entry = {
                        'taxi_id': entry['taxi_id'],
                        'timestamp': entry['timestamp'],
                        'latitude': entry['latitude'],
                        'longitude': entry['longitude']
                    }
                    
                    producer.produce(
                        topic=topic,
                        key=entry['taxi_id'],
                        value=json.dumps(clean_entry),
                        callback=lambda err, msg, seq=messages_sent: delivery_callback_minimal(err, msg, seq)
                    )
                    messages_sent += 1
                    batch_count += 1
                    last_timestamp = dt
                    
                    # Send watermark periodically
                    if messages_sent % 100 == 0:
                        watermark_ts = int(dt.timestamp() * 1000)
                        producer.produce(
                            topic=topic,
                            key="WATERMARK",
                            value=json.dumps({"watermark": watermark_ts})
                        )
                        logger.debug(f"Sent watermark: {watermark_ts}")
                        
                except Exception as e:
                    logger.error(f"Failed to send message for taxi {entry['taxi_id']}: {e}")
            
            data_index += 1
        
        producer.poll(0.1)
        
        # Log progress
        progress = (data_index / len(parsed_data)) * 100
        
        if messages_sent % 20 == 0 or messages_sent < 50:
            logger.info(f"Streaming progress: {progress:.1f}% ({messages_sent} messages sent) - Window: {current_time_window} ({batch_count} records)")
        
        batch_duration = time_module.time() - batch_start
        sleep_time = update_interval - batch_duration
        if sleep_time > 0:
            time_module.sleep(sleep_time)
    
    # watermark
    final_watermark = int(last_timestamp.timestamp() * 1000)
    producer.produce(
        topic=topic,
        key="WATERMARK",
        value=json.dumps({"watermark": final_watermark})
    )
    logger.info(f"Sent final watermark: {final_watermark}")
    
    # Final flush
    producer.flush()
    logger.info(f"Completed streaming {messages_sent} messages in strict chronological order")
    logger.info(f"Final timestamp: {last_timestamp}")

def main():
    input_pattern = '/app/data/*.txt'  
    kafka_topic = 'taxi-locations'
    
    data_dir = '/app/data'
    if not os.path.exists(data_dir):
        logger.error(f"Data directory {data_dir} does not exist!")
        return
    
    # List available files
    all_files = glob.glob(input_pattern)
    logger.info(f"Found {len(all_files)} data files in {data_dir}")
    if len(all_files) == 0:
        logger.error("No data files found! Check the data directory.")
        return
    
    logger.info(f"Producer: Processing ALL {len(all_files)} files")
    logger.info("Kafka & Flink handling massive volumes!")
    start_time = time.time()
    
    processed_files = 0
    total_records_sent = 0
    
    # Process files in large batches with parallelism
    batch_size = 200  
    for i in range(0, len(all_files), batch_size):
        batch_files = all_files[i:i+batch_size]
        batch_start = time.time()
        
        logger.info(f"Processing batch {i//batch_size + 1}/{(len(all_files)-1)//batch_size + 1} ({len(batch_files)} files)")
        
        max_workers = min(16, multiprocessing.cpu_count() * 2) 
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {
                executor.submit(process_single_file, file_path): file_path 
                for file_path in batch_files
            }
            
            batch_data = []
            files_processed_in_batch = 0
            
            # Collect results as they complete
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    file_data = future.result()
                    if file_data:  
                        batch_data.extend(file_data)
                    files_processed_in_batch += 1
                    processed_files += 1
                    
                    if files_processed_in_batch % 50 == 0:
                        logger.info(f"    Processed {files_processed_in_batch}/{len(batch_files)} files in current batch...")
                        
                except Exception as e:
                    logger.error(f"Error processing file {file_path}: {e}")
        
        # TEMPORAL STREAMING
        if batch_data:
            send_start = time.time()
            send_to_kafka_temporal_streaming(batch_data, kafka_topic, speed_multiplier=120) 
            send_time = time.time() - send_start
            total_records_sent += len(batch_data)
            
            batch_time = time.time() - batch_start
            throughput = len(batch_data) / send_time if send_time > 0 else 0
            
            logger.info(f"MEGA-BATCH {i//batch_size + 1} COMPLETE:")
            logger.info(f"Files: {len(batch_files)} processed in {batch_time:.1f}s")
            logger.info(f"Records: {len(batch_data):,} sent in {send_time:.1f}s")
            logger.info(f"Throughput: {throughput:,.0f} records/second")
            logger.info(f"Total progress: {processed_files}/{len(all_files)} files ({100*processed_files/len(all_files):.1f}%)")
        
        time.sleep(0.1)
    
    end_time = time.time()
    total_time = end_time - start_time
    overall_throughput = total_records_sent / total_time if total_time > 0 else 0
    
    logger.info("PROCESSING COMPLETE!")
    logger.info("=" * 60)
    logger.info(f"Total files processed: {processed_files:,}")
    logger.info(f"Total records sent: {total_records_sent:,}")
    logger.info(f"Total time: {total_time:.1f} seconds ({total_time/60:.1f} minutes)")
    logger.info(f"Overall throughput: {overall_throughput:,.0f} records/second")
    logger.info(f"Files per second: {processed_files/total_time:.1f}")
    logger.info("Kafka & Flink handled the load!")

if __name__ == "__main__":
    main()