FROM python:3.11-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY simulator_v2.py .

ENV MQTT_HOST=mqtt-broker
ENV MQTT_PORT=1883

CMD ["python", "simulator_v2.py"]
