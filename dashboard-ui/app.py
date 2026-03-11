import os
import json
import asyncio
from typing import Any, Dict, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import paho.mqtt.client as mqtt

MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

TOPIC_EVENTS    = "edge/events"
TOPIC_INCIDENTS = "edge/incidents"
TOPIC_COMMANDS  = "edge/commands"
TOPIC_REPORTS   = "edge/reports"     # ← LLM full incident reports
TOPIC_SUMMARIES = "edge/summaries"   # ← LLM 10-second narratives

app = FastAPI(title="Edge CEP Dashboard UI")

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def index():
    return FileResponse("static/index.html")

clients: Set[WebSocket] = set()
loop = None

def safe_json_loads(b: bytes) -> Dict[str, Any]:
    try:
        return json.loads(b.decode("utf-8"))
    except Exception:
        return {"raw": b.decode("utf-8", errors="ignore")}

async def broadcast(msg: Dict[str, Any]) -> None:
    dead = []
    for ws in list(clients):
        try:
            await ws.send_json(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)

def on_connect(client, userdata, flags, rc):
    client.subscribe(TOPIC_EVENTS)
    client.subscribe(TOPIC_INCIDENTS)
    client.subscribe(TOPIC_REPORTS)    # ← new
    client.subscribe(TOPIC_SUMMARIES)  # ← new

def on_message(client, userdata, msg):
    data = safe_json_loads(msg.payload)
    payload = {"topic": msg.topic, "data": data}
    if loop:
        asyncio.run_coroutine_threadsafe(broadcast(payload), loop)

mqtt_client = mqtt.Client(client_id="dashboard_ui")
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

@app.on_event("startup")
async def startup():
    global loop
    loop = asyncio.get_running_loop()
    mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    mqtt_client.loop_start()

@app.on_event("shutdown")
async def shutdown():
    mqtt_client.loop_stop()

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        while True:
            msg = await ws.receive_text()
            try:
                obj = json.loads(msg)
                cmd = obj.get("cmd")
                if cmd:
                    mqtt_client.publish(TOPIC_COMMANDS, cmd, qos=0)
            except Exception:
                pass
    except WebSocketDisconnect:
        clients.discard(ws)
