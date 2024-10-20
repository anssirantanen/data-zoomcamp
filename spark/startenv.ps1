.\.venv\Scripts\Activate.ps1
docker-compose up -d
jupyter notebook
Start-Process "http://localhost:8080"