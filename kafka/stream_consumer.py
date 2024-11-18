import faust
from faust_ride import TaxiRide

KAFKA_TOPIC = 'fauts_rides_json'

app = faust.App('faust_rides', broker='kafka://localhost:9092')
topic = app.topic(KAFKA_TOPIC, value_type=TaxiRide)

vendor_rides = app.Table('vendor_rides', default=int)

@app.agent(topic)
async def process(stream):
    async for event in stream.group_by(TaxiRide.vendorId):
        vendor_rides[event.vendorId] += 1

if __name__ == '__main__':
    app.main()