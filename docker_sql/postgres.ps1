docker run -it `
    -e POSTGRES_USER="root" `
    -e POSTGRES_PASSWORD="root" `
    -e POSTGRES_DB="ny_taxi" `
    -v C:\Users\apara\Documents\code\data-zoomcamp\docker_sql\ny_taxi_postgres_data:/var/lib/postgresql/data `
    -p 5432:5432 `
     postgres:13