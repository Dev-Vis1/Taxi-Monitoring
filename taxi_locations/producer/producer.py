import os
import glob
import pandas as pd
from confluent_kafka import Producer
import json
import time

# Kafka producer configuration
kafka_config = {
    'bootstrap.servers': 'kafka:29092',
    'client.id': 'taxi-data-producer'
}
producer = Producer(kafka_config)

# Read all matching data files and include taxi ID
def load_taxi_data(pattern, batch_size=100):
    all_files = glob.glob(pattern)
    for i in range(0, len(all_files), batch_size):
        batch_files = all_files[i:i + batch_size]
        combined_data = []
        for path in batch_files:
            taxi_id = os.path.splitext(os.path.basename(path))[0]
            df = pd.read_csv(path, header=None, names=['timestamp', 'latitude', 'longitude'])
            df['taxi_id'] = taxi_id
            combined_data.extend(df.to_dict(orient='records'))
        yield combined_data

# Sort data by timestamp
def sort_by_timestamp(data):
    return sorted(data, key=lambda entry: entry['timestamp'])

# Send each record to the specified Kafka topic
def send_to_kafka(data, topic, batch_size=1000):
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        for entry in batch:
            try:
                producer.produce(
                    topic=topic,
                    key=entry['taxi_id'],
                    value=json.dumps(entry),
                    callback=on_delivery
                )
            except BufferError:
                print(f'Producer queue is full ({len(producer)} pending messages), retrying...')
                producer.poll(1)
        producer.flush()
        print(f"Batch {i // batch_size + 1} sent successfully.")
        time.sleep(0.5)  # Add a delay between batches

# Callback for message delivery status
def on_delivery(err, msg):
    if err:
        print(f'Delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [Partition {msg.partition()}] at offset {msg.offset()}')


def main():
    input_pattern = 'data/*.txt'
    kafka_topic = 'taxi-locations'

    for data_batch in load_taxi_data(input_pattern):
        sorted_data = sort_by_timestamp(data_batch)
        send_to_kafka(sorted_data, kafka_topic)

if __name__ == "__main__":
    main()
