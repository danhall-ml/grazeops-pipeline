FROM python:3.11-slim

WORKDIR /app
RUN pip install --no-cache-dir psycopg[binary]==3.2.6
COPY main.py /app/main.py
COPY calculation_service /app/calculation_service

CMD ["python", "/app/main.py"]
