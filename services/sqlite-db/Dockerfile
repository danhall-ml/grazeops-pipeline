FROM python:3.11-slim

WORKDIR /app
COPY init_sqlite.py /app/init_sqlite.py

CMD ["sh", "-c", "python /app/init_sqlite.py --schema /inputs/schema.sql --db /data/grazeops.db && tail -f /dev/null"]
