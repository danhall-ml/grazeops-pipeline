FROM python:3.11-slim

WORKDIR /workspace
COPY requirements-dev.txt /tmp/requirements-dev.txt
RUN pip install --no-cache-dir -r /tmp/requirements-dev.txt psycopg[binary]==3.2.6

CMD ["pytest", "-q"]
