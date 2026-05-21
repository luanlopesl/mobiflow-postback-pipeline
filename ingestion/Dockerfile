FROM python:3.12-slim

WORKDIR /app

COPY ingestion/requirements.txt /app/ingestion/requirements.txt
RUN pip install --no-cache-dir -r /app/ingestion/requirements.txt

COPY . /app

EXPOSE 5000

CMD ["sh", "-c", "gunicorn --bind 0.0.0.0:${FLASK_PORT:-5000} ingestion.app:app"]
