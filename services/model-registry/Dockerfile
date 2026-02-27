FROM python:3.11-slim

WORKDIR /app
COPY main.py /app/main.py

EXPOSE 8080
CMD ["python", "/app/main.py", "--host", "0.0.0.0", "--port", "8080", "--registry-dir", "/registry-data"]
