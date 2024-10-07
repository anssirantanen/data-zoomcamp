SELECT passenger_count, trip_distance, PULocationID, DOLocationID, payment_type, fare_amount, tolls_amount, tip_amount
FROM ny_taxi_dataset.taxi_trips WHERE fare_amount != 0;

CREATE OR REPLACE TABLE `ny_taxi_dataset.taxi_trips_ml` (
`passenger_count` INTEGER,
`trip_distance` FLOAT64,
`PULocationID` STRING,
`DOLocationID` STRING,
`payment_type` STRING,
`fare_amount` FLOAT64,
`tolls_amount` FLOAT64,
`tip_amount` FLOAT64
) AS (
SELECT CAST(passenger_count AS INTEGER), trip_distance, cast(PULocationID AS STRING), CAST(DOLocationID AS STRING),
CAST(payment_type AS STRING), fare_amount, tolls_amount, tip_amount
FROM `ny_taxi_dataset.taxi_trips_partitioned` WHERE fare_amount != 0
);

CREATE OR REPLACE MODEL `ny_taxi_dataset.tip_model`
OPTIONS
(model_type='linear_reg',
input_label_cols=['tip_amount'],
DATA_SPLIT_METHOD='AUTO_SPLIT') AS
SELECT
*
FROM
`ny_taxi_dataset.taxi_trips_ml`
WHERE
tip_amount IS NOT NULL;

SELECT * FROM ML.FEATURE_INFO(MODEL `ny_taxi_dataset.tip_model`);

SELECT
*
FROM
ML.EVALUATE(MODEL `ny_taxi_dataset.tip_model`,
(
SELECT
*
FROM
`ny_taxi_dataset.taxi_trips_ml`
WHERE
tip_amount IS NOT NULL
));