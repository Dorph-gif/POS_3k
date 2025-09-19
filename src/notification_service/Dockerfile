FROM python:3.11-slim-buster

WORKDIR /app

COPY src/notification_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8002

CMD ["uvicorn", "src.notification_service.main:app", "--host", "0.0.0.0", "--port", "8002", "--reload"]