FROM python:3.11-slim

WORKDIR /app

# Install deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY cep_engine.py .

# Default MQTT settings (override at runtime)
ENV MQTT_HOST=mqtt-broker
ENV MQTT_PORT=1883

CMD ["python", "cep_engine.py"]
