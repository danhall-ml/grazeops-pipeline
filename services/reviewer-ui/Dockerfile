FROM python:3.11-slim

WORKDIR /app
COPY services/reviewer-ui/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY services/reviewer-ui /app

EXPOSE 8501
CMD ["streamlit", "run", "Service_Tests.py", "--server.address=0.0.0.0", "--server.port=8501"]
