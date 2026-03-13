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

_PAHO_V2 = hasattr(mqtt, "CallbackAPIVersion")

def _subscribe_all(client):
    for topic in [TOPIC_EVENTS, TOPIC_INCIDENTS, TOPIC_REPORTS, TOPIC_SUMMARIES]:
        client.subscribe(topic)
    paho_ver = "v2" if _PAHO_V2 else "v1"
    print(f"[DASH] MQTT connected (paho {paho_ver}) — subscribed to: "
          f"{TOPIC_EVENTS}, {TOPIC_INCIDENTS}, {TOPIC_REPORTS}, {TOPIC_SUMMARIES}")

if _PAHO_V2:
    def on_connect(client, userdata, flags, reason_code, properties=None):
        success = not reason_code.is_failure if hasattr(reason_code, "is_failure") else (reason_code == 0)
        if success:
            _subscribe_all(client)
        else:
            print(f"[DASH] MQTT connection failed: reason_code={reason_code}")
else:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            _subscribe_all(client)
        else:
            print(f"[DASH] MQTT connection failed: rc={rc}")

def on_disconnect(client, userdata, *args):
    print(f"[DASH] MQTT disconnected — will reconnect")

def on_message(client, userdata, msg):
    data = safe_json_loads(msg.payload)
    payload = {"topic": msg.topic, "data": data}
    if loop:
        asyncio.run_coroutine_threadsafe(broadcast(payload), loop)

if _PAHO_V2:
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="dashboard_ui")
else:
    mqtt_client = mqtt.Client(client_id="dashboard_ui")
mqtt_client.on_connect    = on_connect
mqtt_client.on_disconnect = on_disconnect
mqtt_client.on_message    = on_message
print(f"[DASH] paho-mqtt {'v2' if _PAHO_V2 else 'v1'} — connecting to {MQTT_HOST}:{MQTT_PORT}")

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
