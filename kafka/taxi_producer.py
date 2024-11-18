from confluent_kafka import Producer
from ride import Ride
import csv
import json

INPUT_DATA_PATH = './datasets/rides.csv'
BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'rides_json'
config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS
}
key_serializer = lambda key: str(key).encode()
value_serializer = lambda x: json.dumps(x.__dict__, default=str).encode('utf-8')

def delivery_callback(err, msg):
    if err:
        print('ERROR: Message failed delivery: {}'.format(err))
    else:
        print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
           topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

def read_records(resource_path): 
    record_arr = []
    with open(resource_path,"r") as file:
        reader = csv.reader(file)
        header = next(reader)
        for row in reader:
            record_arr.append(Ride(arr=row))
        return record_arr
    
def publish_rides(producer, topic: str, messages: list[Ride]):
        for ride in messages:
            producer.produce(topic=topic, key=key_serializer(ride.pu_location_id), value=value_serializer(ride), callback=delivery_callback)
            producer.poll(0)
            producer.flush()

if __name__ == '__main__':
    producer = Producer(config)
    rides = read_records(resource_path=INPUT_DATA_PATH)
    publish_rides(producer, topic=KAFKA_TOPIC, messages=rides)