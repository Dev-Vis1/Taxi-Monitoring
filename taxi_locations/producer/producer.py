import os
import glob
import pandas as pd
from confluent_kafka import Producer
import json

# Kafka producer configuration
kafka_config = {
    'bootstrap.servers': 'kafka:29092',
    'client.id': 'taxi-data-producer'
}
producer = Producer(kafka_config)

# Read all matching data files and include taxi ID
def load_taxi_data(pattern):
    combined_data = []
    for path in glob.glob(pattern):
        taxi_id = os.path.splitext(os.path.basename(path))[0]
        df = pd.read_csv(path, header=None, names=['timestamp', 'latitude', 'longitude'])
        df['taxi_id'] = taxi_id
        combined_data.extend(df.to_dict(orient='records'))
    return combined_data

# Sort data by timestamp
def sort_by_timestamp(data):
    return sorted(data, key=lambda entry: entry['timestamp'])

# Send each record to the specified Kafka topic
def send_to_kafka(data, topic):
    for entry in data:
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

# Callback for message delivery status
def on_delivery(err, msg):
    if err:
        print(f'Delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [Partition {msg.partition()}]')


def main():
    input_pattern = 'data/*.txt'
    kafka_topic = 'taxi-locations'

    data = load_taxi_data(input_pattern)
    sorted_data = sort_by_timestamp(data)
    send_to_kafka(sorted_data, kafka_topic)

if __name__ == "__main__":
    main()
