--table non empty
SELECT * FROM `savvy-primacy-436914-v0.ny_taxi_dataset.taxi_trips` LIMIT 10


-- partiotioned taxi trips
CREATE OR REPLACE TABLE ny_taxi_dataset.taxi_trips_partitioned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM savvy-primacy-436914-v0.ny_taxi_dataset.taxi_trips;

--2s processing time, 118MB data scanned of 982.48 MB
SELECT DISTINCT(VendorID)
FROM ny_taxi_dataset.taxi_trips
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

--0s processing time, 32B data scanned of 982.48 MB
SELECT DISTINCT(VendorID)
FROM ny_taxi_dataset.taxi_trips_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';


--total rows 7696617, avg partition size 280000
SELECT table_name, partition_id, total_rows
FROM `ny_taxi_dataset.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'taxi_trips_partitioned'
ORDER BY total_rows DESC;


CREATE OR REPLACE TABLE ny_taxi_dataset.taxi_trips_partitioned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM taxi-rides-ny.nytaxi.external_yellow_tripdata;

SELECT count(*) as trips
FROM ny_taxi_dataset.taxi_trips_partitioned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-01-01' AND '2029-12-31'
  AND VendorID=1;

SELECT count(*) as trips
FROM ny_taxi_dataset.taxi_trips_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-01-01' AND '2029-12-31'
  AND VendorID=1;
