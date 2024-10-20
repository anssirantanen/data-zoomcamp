docker compose build 
docker compose run dbt-bq-dtc init
docker compose run --workdir="//usr/app/dbt/ny_taxi_dataset" dbt-bq-dtc debug