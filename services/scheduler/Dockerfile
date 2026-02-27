FROM python:3.11-slim

WORKDIR /app
RUN pip install --no-cache-dir psycopg[binary]==3.2.6
COPY services/scheduler/scheduler.py /app/scheduler.py
COPY services/ingestion-worker/main.py /app/worker/main.py
COPY services/ingestion-worker/ingestion_worker /app/worker/ingestion_worker

CMD ["python", "/app/scheduler.py"]
