FROM python:3.11-slim

WORKDIR /app
COPY services/staging-service/main.py /app/main.py
COPY services/calculation-service /app/services/calculation-service

CMD ["python", "/app/main.py"]
