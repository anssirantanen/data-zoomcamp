import csv
from json import dumps
from kafka import KafkaProducer
from time import sleep

KAFKA_TOPIC = 'fauts_rides_json'


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         key_serializer=lambda x: dumps(x).encode('utf-8'),
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

file = open('datasets/rides.csv')

csvreader = csv.reader(file)
header = next(csvreader)
for row in csvreader:
    key = {"vendorId": int(row[0])}
    value = {"vendorId": int(row[0]), "passenger_count": int(row[3]), "trip_distance": float(row[4]), "payment_type": int(row[9]), "total_amount": float(row[16])}
    producer.send(KAFKA_TOPIC, value=value, key=key)
    print("producing")
    sleep(1)