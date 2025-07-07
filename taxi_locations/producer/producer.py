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

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Ultra-high-performance Kafka producer configuration
kafka_config = {
    'bootstrap.servers': 'kafka:29092',
    'client.id': 'taxi-data-producer',
    'acks': '0',  # Fire-and-forget for maximum throughput
    'compression.type': 'lz4',  # Fast compression
    'batch.size': 500000,  # Large batch size (500KB)
    'batch.num.messages': 50000,  # More messages per batch
    'linger.ms': 10,  # Small linger for responsiveness
    'queue.buffering.max.messages': 1000000,  # Large buffer
    'queue.buffering.max.ms': 50,  # Quick flush
    'message.max.bytes': 10000000,
    'socket.keepalive.enable': True,
    'socket.send.buffer.bytes': 131072,  # 128KB send buffer
    'socket.receive.buffer.bytes': 65536,  # 64KB receive buffer
    'enable.idempotence': False,  # Disable for maximum speed
    'max.in.flight.requests.per.connection': 10  # More parallel requests
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
    """Ultra-fast file processing with minimal overhead"""
    try:
        taxi_id = os.path.splitext(os.path.basename(file_path))[0]
        
        # ULTRA-FAST: Read file directly without chunking for small-medium files
        try:
            df = pd.read_csv(file_path, 
                           header=None,
                           names=['taxi_id_file', 'timestamp', 'longitude', 'latitude'],
                           dtype={'taxi_id_file': 'str', 'longitude': 'float64', 'latitude': 'float64'})
        except Exception:
            # Fallback to chunked reading for very large files
            chunks = []
            for chunk in pd.read_csv(file_path, 
                                   header=None,
                                   names=['taxi_id_file', 'timestamp', 'longitude', 'latitude'],
                                   chunksize=10000):  # Larger chunks
                chunks.append(chunk)
            
            if not chunks:
                return []
            df = pd.concat(chunks, ignore_index=True)
        
        # FAST CLEANING: Vectorized operations
        initial_count = len(df)
        
        # Remove invalid coordinates in one step
        df = df[(df['latitude'] != 0) & (df['longitude'] != 0) & 
                df['latitude'].notna() & df['longitude'].notna()]
        
        # Quick duplicate removal on key columns only
        df = df.drop_duplicates(subset=['timestamp', 'longitude', 'latitude'], keep='first')
        
        if len(df) == 0:
            return []
        
        # Set taxi_id and return optimized format
        df['taxi_id'] = taxi_id
        
        # Return as records with minimal processing
        return df[['timestamp', 'latitude', 'longitude', 'taxi_id']].to_dict(orient='records')
        
    except Exception as e:
        logger.debug(f"Error processing {file_path}: {e}")  # Debug level to reduce noise
        return []

# Sort data by timestamp
def sort_by_timestamp(data):
    return sorted(data, key=lambda entry: entry['timestamp'])

# REAL-TIME TEMPORAL STREAMING - Enhanced for realistic taxi movement
def send_to_kafka_temporal_streaming(data, topic, speed_multiplier=120):
    logger.info(f"Starting temporal streaming for {len(data)} records with {speed_multiplier}x speed")
    
    if not data:
        return
    
    # GLOBAL TEMPORAL ORDERING: Sort ALL data by timestamp across all taxis
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
    
    # CRITICAL: Sort by timestamp globally to ensure chronological order
    parsed_data.sort(key=lambda x: x[0])
    logger.info(f"Sorted {len(parsed_data)} records chronologically")
    
    # Calculate the time range and streaming parameters
    start_time = parsed_data[0][0]
    end_time = parsed_data[-1][0]
    total_duration = (end_time - start_time).total_seconds()
    
    logger.info(f"Data time range: {start_time} to {end_time} ({total_duration:.0f} seconds)")
    logger.info(f"Streaming duration: {total_duration/speed_multiplier:.1f} seconds")
    
    # Process data in chronological order with proper timing
    import time as time_module
    messages_sent = 0
    streaming_start = time_module.time()
    last_log_time = streaming_start
    
    # Track last watermark sent time
    last_watermark_time = streaming_start
    
    for i, (dt, entry) in enumerate(parsed_data):
        # Calculate proper timing to maintain chronological order
        time_since_start = (dt - start_time).total_seconds()
        target_real_time = streaming_start + (time_since_start / speed_multiplier)
        current_real_time = time_module.time()
        
        # Wait if needed to maintain proper timing
        if target_real_time > current_real_time:
            sleep_time = target_real_time - current_real_time
            if sleep_time > 0.01:  # Only sleep for significant delays
                time_module.sleep(sleep_time)
        
        try:
            # Send only the core fields that TaxiLocation expects
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
            
            # Send watermark every 100 messages or 5 seconds
            if messages_sent % 100 == 0 or (time_module.time() - last_watermark_time) > 5:
                watermark_ts = int(dt.timestamp() * 1000)  # UTC milliseconds
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
        if time_module.time() - last_log_time > 5:  # Log every 5 seconds
            progress = (i + 1) / len(parsed_data) * 100
            logger.info(f"Streaming progress: {progress:.1f}% ({messages_sent} messages sent)")
            last_log_time = time_module.time()
    
    # Send final watermark and end markers
    final_watermark = int(end_time.timestamp() * 1000)
    producer.produce(
        topic=topic,
        key="WATERMARK",
        value=json.dumps({"watermark": final_watermark})
    )
    logger.info(f"Sent final watermark: {final_watermark}")
    
    # Final flush and summary
    producer.flush()
    total_time = time_module.time() - streaming_start
    logger.info(f"Temporal streaming completed: {messages_sent} messages sent in {total_time:.2f}s")
    logger.info(f"Average rate: {messages_sent/total_time:.1f} messages/second")

def delivery_callback_minimal(err, msg, sequence):
    """Minimal callback for better performance"""
    if err:
        logger.error(f'Delivery failed for message {sequence}: {err}')
    elif sequence % 200 == 0:  # Log every 200th message
        logger.info(f'Smooth streaming: delivered message {sequence}')

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

def send_to_kafka_every_2_seconds(data, topic, update_interval=2.0):
    """Send taxi updates every 2 seconds for smooth real-time visualization with GLOBAL chronological order"""
    if not data:
        logger.warning("No data to send")
        return
    
    logger.info(f"Starting real-time streaming with {update_interval}s updates")
    
    # Parse and sort data chronologically GLOBALLY (all taxis together)
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
        
    # CRITICAL: Sort ALL data globally by timestamp to ensure chronological order
    parsed_data.sort(key=lambda x: x[0])
    
    start_time = parsed_data[0][0]
    end_time = parsed_data[-1][0]
    total_duration = (end_time - start_time).total_seconds()
    
    logger.info(f"Data time range: {start_time} to {end_time} ({total_duration:.0f} seconds)")
    logger.info(f"Will stream {len(parsed_data)} records in chronological order with {update_interval}s interval updates")
    
    # Stream data in STRICT chronological order
    import time as time_module
    messages_sent = 0
    streaming_start = time_module.time()
    
    # Keep track of last timestamp sent to ensure we never go backwards
    last_timestamp = None
    
    # Process records in batches every 2 seconds, but maintain chronological order
    data_index = 0
    
    while data_index < len(parsed_data):
        batch_start = time_module.time()
        batch_count = 0
        
        # Send a batch of records for this time window (next 30 seconds of data time)
        current_time_window = parsed_data[data_index][0]
        window_end = current_time_window + timedelta(seconds=30)
        
        # Send all records within this 30-second window
        while data_index < len(parsed_data) and parsed_data[data_index][0] <= window_end:
            dt, entry = parsed_data[data_index]
            
            # Ensure we never go backwards in time
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
        
        # Flush producer to ensure delivery
        producer.poll(0.1)
        
        # Log progress
        progress = (data_index / len(parsed_data)) * 100
        
        if messages_sent % 20 == 0 or messages_sent < 50:
            logger.info(f"Streaming progress: {progress:.1f}% ({messages_sent} messages sent) - Window: {current_time_window} ({batch_count} records)")
        
        # Wait for the next update interval
        batch_duration = time_module.time() - batch_start
        sleep_time = update_interval - batch_duration
        if sleep_time > 0:
            time_module.sleep(sleep_time)
    
    # Final watermark
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
    input_pattern = '/app/data/*.txt'  # Fixed path for Docker container
    kafka_topic = 'taxi-locations'
    
    # Check if data directory exists
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
    
    logger.info(f"🚀 ULTRA-HIGH-PERFORMANCE Producer: Processing ALL {len(all_files)} files")
    logger.info("🔥 Kafka & Flink can handle massive volumes - let's prove it!")
    start_time = time.time()
    
    # ULTRA-HIGH-PERFORMANCE: Process ALL files with maximum throughput
    processed_files = 0
    total_records_sent = 0
    
    # Process files in large batches with maximum parallelism
    batch_size = 200  # Process 200 files at a time for maximum throughput
    for i in range(0, len(all_files), batch_size):
        batch_files = all_files[i:i+batch_size]
        batch_start = time.time()
        
        logger.info(f"🚀 Processing MEGA-BATCH {i//batch_size + 1}/{(len(all_files)-1)//batch_size + 1} ({len(batch_files)} files)")
        
        # PARALLEL PROCESSING: Use ALL available CPU cores
        max_workers = min(16, multiprocessing.cpu_count() * 2)  # Aggressive parallelism
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit ALL files in batch for parallel processing
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
                    if file_data:  # Only add non-empty files
                        batch_data.extend(file_data)
                    files_processed_in_batch += 1
                    processed_files += 1
                    
                    # Progress indicator for large batches
                    if files_processed_in_batch % 50 == 0:
                        logger.info(f"    📈 Processed {files_processed_in_batch}/{len(batch_files)} files in current batch...")
                        
                except Exception as e:
                    logger.error(f"❌ Error processing file {file_path}: {e}")
        
        # TEMPORAL STREAMING: Send data in real-time chronological order
        if batch_data:
            send_start = time.time()
            send_to_kafka_temporal_streaming(batch_data, kafka_topic, speed_multiplier=120)  # 3600x faster (1 hour in 1 second)
            send_time = time.time() - send_start
            total_records_sent += len(batch_data)
            
            batch_time = time.time() - batch_start
            throughput = len(batch_data) / send_time if send_time > 0 else 0
            
            logger.info(f"✅ MEGA-BATCH {i//batch_size + 1} COMPLETE:")
            logger.info(f"    📁 Files: {len(batch_files)} processed in {batch_time:.1f}s")
            logger.info(f"    📊 Records: {len(batch_data):,} sent in {send_time:.1f}s")
            logger.info(f"    🚀 Throughput: {throughput:,.0f} records/second")
            logger.info(f"    📈 Total progress: {processed_files}/{len(all_files)} files ({100*processed_files/len(all_files):.1f}%)")
        
        # Minimal pause to let Kafka catch up, but keep pressure high
        time.sleep(0.1)
    
    end_time = time.time()
    total_time = end_time - start_time
    overall_throughput = total_records_sent / total_time if total_time > 0 else 0
    
    logger.info("🎉 ULTRA-HIGH-PERFORMANCE PROCESSING COMPLETE!")
    logger.info("=" * 60)
    logger.info(f"📁 Total files processed: {processed_files:,}")
    logger.info(f"📊 Total records sent: {total_records_sent:,}")
    logger.info(f"⏱️  Total time: {total_time:.1f} seconds ({total_time/60:.1f} minutes)")
    logger.info(f"🚀 Overall throughput: {overall_throughput:,.0f} records/second")
    logger.info(f"📈 Files per second: {processed_files/total_time:.1f}")
    logger.info("🔥 Kafka & Flink handled the load like champions!")

if __name__ == "__main__":
    main()