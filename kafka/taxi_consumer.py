from confluent_kafka import Consumer
from ride import Ride
import json 
BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'rides_json'

config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': 'taxi-ride-consumer',
    'auto.offset.reset': 'earliest'
}

if __name__ == '__main__':
    consumer = Consumer(config)
    consumer.subscribe([KAFKA_TOPIC])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg == {}:

                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                print(msg.value().decode('utf-8'))
                key=msg.key().decode('utf-8')
                raw_value = msg.value().decode('utf-8')
                value=Ride.from_dict(json.loads(raw_value))
                print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                    topic=msg.topic(), key=key, value=raw_value))
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()