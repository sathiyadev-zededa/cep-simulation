# llm_service.py
#
# LLM Incident Intelligence Service
#
# Subscribes to:
#   edge/events    → buffers raw events → every 10s sends to Ollama for real-time narrative
#   edge/incidents → queries ChromaDB for similar past incidents → Ollama generates full report
#
# Publishes to:
#   edge/summaries → 10-second rolling narrative of what is happening right now
#   edge/reports   → full structured incident report with root cause, impact, actions
#
# Architecture:
#   MQTT → on_event() / on_incident()
#              ↓
#         EventBuffer (10s rolling window)
#         IncidentMemory (fastembed + numpy vector store)
#              ↓
#         Ollama (local LLM — llama3.2 / mistral / phi3)
#              ↓
#         MQTT publish → edge/summaries, edge/reports
#
# Requirements:
#   pip install paho-mqtt chromadb sentence-transformers ollama
#
# Prerequisites:
#   1. Ollama installed and running: https://ollama.com
#   2. MQTT broker running (same as rest of stack)
#
# Model download is AUTOMATIC — no manual "ollama pull" needed.
# On first startup the service detects the model is missing and pulls it.
#
# Usage:
#   export MQTT_HOST=127.0.0.1
#   export MQTT_PORT=1883
#   export OLLAMA_HOST=http://localhost:11434
#   export OLLAMA_MODEL=llama3.2
#   python llm_service.py

import json
import os
import threading
import time
import uuid
from collections import deque
from datetime import datetime
from typing import Any, Dict, List, Optional

import chromadb
import ollama
import paho.mqtt.client as mqtt
from chromadb.utils import embedding_functions

# ─── PAHO v1 / v2 COMPAT ─────────────────────────────────────────────────────
_PAHO_V2 = hasattr(mqtt, "CallbackAPIVersion")

# ─── CONFIG ──────────────────────────────────────────────────────────────────

MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

TOPIC_EVENTS       = "edge/events"
TOPIC_INCIDENTS    = "edge/incidents"
TOPIC_FAULT_EVENTS = "edge/fault_events"   # aggregated CEP fault events (new)
TOPIC_SUMMARIES    = "edge/summaries"
TOPIC_REPORTS      = "edge/reports"

OLLAMA_HOST  = os.getenv("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2")

# How often to generate a real-time narrative summary (seconds)
SUMMARY_INTERVAL_S = 10

# How many similar past incidents to retrieve from ChromaDB for RAG context
RAG_TOP_K = 3

# Where ChromaDB stores its data (persists between restarts)
CHROMA_PATH = os.getenv("CHROMA_PATH", "./incident_store")

SITE_ID = os.getenv("SITE_ID", "SUBSTATION-ABU-01")


# ─── UTILITIES ───────────────────────────────────────────────────────────────

def now_ms() -> int:
    return int(time.time() * 1000)


def now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def make_event(event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id":         f"{now_ms()}-{uuid.uuid4().hex[:6]}",
        "event_type": event_type,
        "ts_ms":      now_ms(),
        "site_id":    SITE_ID,
        "source":     "llm_service",
        "payload":    payload,
    }


def pub(client: mqtt.Client, topic: str, event_type: str, payload: Dict[str, Any]) -> None:
    evt = make_event(event_type, payload)
    client.publish(topic, json.dumps(evt, ensure_ascii=False, separators=(",", ":")), qos=0)
    print(f"[PUB] {topic} → {event_type}")


# ─── EVENT BUFFER ─────────────────────────────────────────────────────────────
#
# Collects all raw events from edge/events in a sliding 10-second window.
# The summarisation loop drains this buffer every SUMMARY_INTERVAL_S seconds
# and sends the contents to Ollama for a plain-English narrative.

class EventBuffer:
    def __init__(self, max_age_s: float = 60.0):
        self._lock    = threading.Lock()
        self._buffer: deque = deque()
        self._max_age = max_age_s

    def add(self, evt: Dict[str, Any]) -> None:
        with self._lock:
            evt["_recv_s"] = time.time()
            self._buffer.append(evt)
            self._prune()

    def drain(self) -> List[Dict[str, Any]]:
        """Return and clear all buffered events."""
        with self._lock:
            self._prune()
            events = list(self._buffer)
            self._buffer.clear()
            return events

    def peek(self, last_n_s: float = 30.0) -> List[Dict[str, Any]]:
        """Return recent events without clearing."""
        with self._lock:
            self._prune()
            cutoff = time.time() - last_n_s
            return [e for e in self._buffer if e.get("_recv_s", 0) >= cutoff]

    def _prune(self) -> None:
        cutoff = time.time() - self._max_age
        while self._buffer and self._buffer[0].get("_recv_s", 0) < cutoff:
            self._buffer.popleft()

    def __len__(self) -> int:
        with self._lock:
            return len(self._buffer)


# ─── INCIDENT MEMORY (ChromaDB) ───────────────────────────────────────────────
#
# Stores every generated incident report as a vector embedding.
# When a new incident arrives, the most similar past incidents are retrieved
# and included in the Ollama prompt — this is the RAG layer.

class IncidentMemory:
    def __init__(self, persist_path: str = CHROMA_PATH):
        print(f"[CHROMA] Initialising vector store at {persist_path}")
        self._client = chromadb.PersistentClient(path=persist_path)

        # Use a local sentence transformer — no API key, runs offline
        self._ef = embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name="all-MiniLM-L6-v2"
        )

        self._collection = self._client.get_or_create_collection(
            name="incidents",
            embedding_function=self._ef,
            metadata={"hnsw:space": "cosine"},
        )
        print(f"[CHROMA] Collection loaded — {self._collection.count()} existing incidents")

    def store(self, incident: Dict[str, Any], report: str) -> str:
        """
        Store an incident + its LLM report in ChromaDB.
        Returns the document ID.
        """
        doc_id = f"inc-{uuid.uuid4().hex}"
        doc_text = self._incident_to_text(incident, report)

        self._collection.add(
            documents=[doc_text],
            metadatas=[{
                "incident_type":  incident.get("payload", {}).get("incident_type", "UNKNOWN"),
                "confidence":     str(incident.get("payload", {}).get("confidence", 0)),
                "site_id":        incident.get("site_id", SITE_ID),
                "feeder_id":      incident.get("feeder_id", ""),
                "transformer_id": incident.get("transformer_id", ""),
                "ts_ms":          str(incident.get("ts_ms", now_ms())),
                "timestamp_iso":  now_iso(),
            }],
            ids=[doc_id],
        )
        print(f"[CHROMA] Stored incident {doc_id} — total: {self._collection.count()}")
        return doc_id

    def find_similar(self, incident: Dict[str, Any], n: int = RAG_TOP_K) -> List[Dict]:
        """
        Find the most similar past incidents using vector similarity.
        Returns list of {"text": ..., "metadata": ...} dicts.
        """
        if self._collection.count() == 0:
            return []

        query = self._incident_to_query(incident)
        try:
            results = self._collection.query(
                query_texts=[query],
                n_results=min(n, self._collection.count()),
                include=["documents", "metadatas", "distances"],
            )
            similar = []
            for doc, meta, dist in zip(
                results["documents"][0],
                results["metadatas"][0],
                results["distances"][0],
            ):
                if dist < 0.8:
                    similar.append({"text": doc, "metadata": meta, "distance": dist})
            return similar
        except Exception as e:
            print(f"[CHROMA] Query error: {e}")
            return []

    def _incident_to_text(self, incident: Dict, report: str) -> str:
        p = incident.get("payload", {})
        d = p.get("details", {})
        return (
            f"Incident Type: {p.get('incident_type', 'UNKNOWN')}\n"
            f"Site: {incident.get('site_id', '')}\n"
            f"Feeder: {incident.get('feeder_id', '')}\n"
            f"Transformer: {incident.get('transformer_id', '')}\n"
            f"Confidence: {p.get('confidence', 0)}\n"
            f"Summary: {p.get('summary', '')}\n"
            f"Details: {json.dumps(d)}\n"
            f"Timestamp: {now_iso()}\n"
            f"Report:\n{report}"
        )

    def _incident_to_query(self, incident: Dict) -> str:
        p = incident.get("payload", {})
        return (
            f"{p.get('incident_type', 'UNKNOWN')} "
            f"feeder {incident.get('feeder_id', '')} "
            f"transformer {incident.get('transformer_id', '')} "
            f"confidence {p.get('confidence', 0)} "
            f"{p.get('summary', '')}"
        )


# ─── OLLAMA CLIENT ────────────────────────────────────────────────────────────
#
# Wraps the ollama Python library.
# On startup it checks whether the configured model is already downloaded.
# If not, it pulls it automatically — no manual "ollama pull" needed.
# Handles connection errors gracefully so the service never crashes if
# Ollama is temporarily unavailable.

class OllamaClient:
    def __init__(self, host: str = OLLAMA_HOST, model: str = OLLAMA_MODEL):
        self.model  = model
        self.host   = host
        # Always use an explicit Client instance so the host is honoured
        # regardless of the ollama library version (env var is unreliable in v0.2+)
        self._client = ollama.Client(host=host)
        print(f"[OLLAMA] Client created → host={host}  model={model}")
        # Auto-pull the model if it is not already downloaded
        self._ensure_model()

    # ── Model management ─────────────────────────────────────────────────────

    def _ensure_model(self) -> None:
        """
        Check if the model is already downloaded.
        If not, pull it automatically with a live progress display.
        Safe to call every startup — skips download if model already exists.
        """
        if not self.is_available():
            print("[OLLAMA] Server not reachable — skipping model check.")
            print(f"[OLLAMA] Make sure Ollama is running at {self.host}")
            return

        if self._model_exists():
            print(f"[OLLAMA] Model '{self.model}' already downloaded — ready.")
            return

        print(f"[OLLAMA] Model '{self.model}' not found — downloading now...")
        print("[OLLAMA] This may take a few minutes depending on your connection.")
        self._pull_model()

    def _model_exists(self) -> bool:
        """Return True if the model is present in Ollama's local store.
        Uses self._client so the correct remote host is always contacted."""
        try:
            local_models = self._client.list()
            # ollama >= 0.2 returns a Pydantic ListResponse; older returns a dict
            if isinstance(local_models, dict):
                models_list = local_models.get("models", [])
                names = [m.get("name", "") for m in models_list]
            else:
                models_list = getattr(local_models, "models", [])
                names = [
                    getattr(m, "model", getattr(m, "name", ""))
                    for m in models_list
                ]
            print(f"[OLLAMA] Models on server: {names}")
            return any(self.model in name for name in names)
        except Exception as e:
            print(f"[OLLAMA] Could not list models: {e}")
            return False

    def _pull_model(self) -> None:
        """Pull (download) the model via self._client so the correct host is used."""
        try:
            print(f"[OLLAMA] Pulling '{self.model}' from {self.host} ...")
            last_status = ""
            for progress in self._client.pull(self.model, stream=True):
                # Handle both dict and Pydantic progress objects
                if isinstance(progress, dict):
                    status    = progress.get("status", "")
                    total     = progress.get("total", 0)
                    completed = progress.get("completed", 0)
                else:
                    status    = getattr(progress, "status", "")
                    total     = getattr(progress, "total", 0) or 0
                    completed = getattr(progress, "completed", 0) or 0

                if status != last_status:
                    if total and completed:
                        pct = (completed / total) * 100
                        print(f"[OLLAMA] {status} — {pct:.1f}%")
                    else:
                        print(f"[OLLAMA] {status}")
                    last_status = status

            print(f"[OLLAMA] Model '{self.model}' downloaded successfully.")

        except Exception as e:
            print(f"[OLLAMA] Pull failed: {e}")
            print(f"[OLLAMA] Pull manually: kubectl exec <ollama-pod> -- ollama pull {self.model}")

    # ── Inference ─────────────────────────────────────────────────────────────

    def chat(self, system_prompt: str, user_prompt: str,
             max_tokens: int = 600) -> Optional[str]:
        """Send a prompt to Ollama and return the response text.
        Uses self._client so the correct remote host is always contacted."""
        try:
            response = self._client.chat(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_prompt},
                ],
                options={"num_predict": max_tokens, "temperature": 0.3},
            )
            # Handle both dict (old) and Pydantic (new) response formats
            if isinstance(response, dict):
                return response["message"]["content"].strip()
            else:
                return response.message.content.strip()
        except Exception as e:
            print(f"[OLLAMA] Chat error: {e}")
            return None

    def is_available(self) -> bool:
        """Return True if the Ollama server at self.host is reachable."""
        try:
            self._client.list()
            return True
        except Exception:
            return False


# ─── PROMPT BUILDERS ──────────────────────────────────────────────────────────

def build_summary_prompt(events: List[Dict]) -> tuple[str, str]:
    """Build system + user prompt for real-time event summarisation."""

    system = (
        "You are an operations analyst for a power distribution substation. "
        "You receive raw sensor and protection relay events and explain what is "
        "happening in plain English for a control room operator. "
        "Be concise, factual, and use operational terminology. "
        "Focus on what the operator needs to know right now. "
        "Maximum 3 sentences."
    )

    # Format events compactly for the prompt
    event_lines = []
    for e in events[-20:]:   # last 20 events max to stay within token limits
        et  = e.get("event_type", "unknown")
        p   = e.get("payload", {})
        fid = e.get("feeder_id", "")
        ts  = e.get("ts_ms", 0)
        event_lines.append(f"  [{et}] feeder={fid} {json.dumps(p)}")

    user = (
        f"The following events occurred in the last {SUMMARY_INTERVAL_S} seconds "
        f"at {SITE_ID}:\n\n"
        + "\n".join(event_lines)
        + "\n\nDescribe what is happening in plain English."
    )

    return system, user


def build_report_prompt(
    incident: Dict,
    similar: List[Dict],
    recent_events: List[Dict],
) -> tuple[str, str]:
    """Build system + user prompt for full incident report generation."""

    system = (
        "You are a senior power systems engineer and incident analyst. "
        "You write structured incident reports for distribution substation faults. "
        "Your reports are technical but clear, actionable, and reference historical precedent where available. "
        "Always output your report in the exact structure requested."
    )

    # Incident data
    p  = incident.get("payload", {})
    d  = p.get("details", {})
    incident_block = (
        f"INCIDENT CLASSIFICATION\n"
        f"  Type:        {p.get('incident_type', 'UNKNOWN')}\n"
        f"  Confidence:  {p.get('confidence', 0) * 100:.0f}%\n"
        f"  Summary:     {p.get('summary', '')}\n"
        f"  Site:        {incident.get('site_id', SITE_ID)}\n"
        f"  Feeder:      {incident.get('feeder_id', 'N/A')}\n"
        f"  Transformer: {incident.get('transformer_id', 'N/A')}\n"
        f"  Details:     {json.dumps(d, indent=2)}\n"
        f"  Time:        {now_iso()}\n"
    )

    # Recent raw events context (last 30s leading up to the incident)
    event_block = ""
    if recent_events:
        lines = []
        for e in recent_events[-15:]:
            lines.append(
                f"  [{e.get('event_type')}] {e.get('feeder_id','')} "
                f"payload={json.dumps(e.get('payload', {}))}"
            )
        event_block = "\nRECENT EVENTS (leading up to incident):\n" + "\n".join(lines)

    # RAG context from ChromaDB
    rag_block = ""
    if similar:
        rag_lines = []
        for i, s in enumerate(similar, 1):
            meta = s.get("metadata", {})
            rag_lines.append(
                f"\n  Past Incident {i} "
                f"(similarity: {(1 - s.get('distance', 1)) * 100:.0f}%, "
                f"recorded: {meta.get('timestamp_iso', 'unknown')}):\n"
                + "\n".join(f"    {line}" for line in s["text"].split("\n")[:12])
            )
        rag_block = "\nSIMILAR PAST INCIDENTS (from historical database):\n" + "".join(rag_lines)
    else:
        rag_block = "\nSIMILAR PAST INCIDENTS: None on record yet — this may be the first occurrence."

    user = (
        f"{incident_block}"
        f"{event_block}"
        f"{rag_block}"
        f"\n\nGenerate a structured incident report with exactly these sections:\n"
        f"1. ROOT CAUSE ANALYSIS — What most likely caused this incident based on the telemetry\n"
        f"2. IMMEDIATE IMPACT — Which assets and customers are affected right now\n"
        f"3. RECOMMENDED ACTIONS — Step-by-step actions for the operator, in priority order\n"
        f"4. ESTIMATED RESOLUTION TIME — Based on incident type and historical precedent\n"
        f"5. HISTORICAL COMPARISON — Reference to similar past incidents if available\n"
        f"6. PREVENTIVE MEASURES — What can be done to reduce recurrence\n"
        f"\nKeep each section concise. Total report under 350 words."
    )

    return system, user


def build_fault_report_prompt(
    fault_event: Dict,
    similar: List[Dict],
    recent_events: List[Dict],
    recent_incidents: List[Dict],
) -> tuple[str, str]:
    """Build a temporal-narrative prompt for an aggregated FaultEvent.

    Produces a report describing the full progression of the fault —
    which incidents fired, in what order, how telemetry values changed —
    rather than just a snapshot of the final state.
    """

    system = (
        "You are a senior power systems engineer and incident analyst at a distribution substation. "
        "You write precise, technical incident reports based on CEP (Complex Event Processing) data. "
        "Your strength is describing HOW a fault developed over time — the sequence of events, "
        "the rate of change in readings, and the causal chain from first signal to final alarm. "
        "Write in clear prose. Be specific about timestamps and values. Avoid generic filler text."
    )

    root_cause = fault_event.get("root_cause", "UNKNOWN")
    severity   = fault_event.get("severity", "UNKNOWN")
    confidence = fault_event.get("confidence", 0.0)
    message    = fault_event.get("message", "")
    feeder_id  = fault_event.get("feeder_id", "N/A")
    factors    = fault_event.get("contributing_factors", [])
    inc_count  = fault_event.get("incident_count", 1)
    fault_ts   = fault_event.get("ts_ms", 0)
    fault_iso  = datetime.utcfromtimestamp(fault_ts / 1000).strftime("%H:%M:%SZ") if fault_ts else "unknown"

    # CEP pattern context
    if "SUSTAINED" in root_cause:
        pattern_desc = (
            "CEP Pattern A — Sustained Condition: the fault condition persisted continuously "
            "for 60+ seconds before this alarm fired. This is NOT a transient spike. "
            "Describe the gradual progression visible in the telemetry timeline below."
        )
    elif "REPEATED" in root_cause or "TRANSIENT" in root_cause:
        pattern_desc = (
            "CEP Pattern B — Repeated Events: the same fault occurred multiple times "
            "within a short window, indicating a recurring/intermittent fault."
        )
    elif "LOCKOUT" in root_cause or "PERMANENT" in root_cause:
        pattern_desc = (
            "CEP Pattern B — Breaker Lockout: multiple failed auto-reclose attempts "
            "confirm a permanent fault on the feeder."
        )
    else:
        pattern_desc = (
            "CEP Aggregated Fault: multiple correlated incidents were grouped into "
            "a single root-cause event by the edge CEP engine."
        )

    fault_block = (
        f"FAULT EVENT\n"
        f"  Root Cause:           {root_cause}\n"
        f"  Severity:             {severity}\n"
        f"  Confidence:           {confidence * 100:.0f}%\n"
        f"  Final Message:        {message}\n"
        f"  Feeder:               {feeder_id}\n"
        f"  Site:                 {SITE_ID}\n"
        f"  Alarm Time:           {fault_iso}\n"
        f"  Contributing Factors: {', '.join(factors) if factors else 'none'}\n"
        f"  Total Incidents:      {inc_count}\n"
        f"\nCEP DETECTION PATTERN:\n  {pattern_desc}\n"
    )

    # ── Incident timeline ──────────────────────────────────────────────────────
    timeline_block = ""
    if recent_incidents:
        sorted_incidents = sorted(
            recent_incidents,
            key=lambda e: e.get("ts_ms", e.get("payload", {}).get("ts_ms", 0))
        )
        lines = []
        for inc in sorted_incidents:
            p       = inc.get("payload", {})
            ts      = inc.get("ts_ms", 0)
            iso     = datetime.utcfromtimestamp(ts / 1000).strftime("%H:%M:%SZ") if ts else "?"
            inc_type = p.get("incident_type", "?")
            sev     = p.get("severity", "?")
            summary = p.get("summary", "")
            lines.append(f"  {iso}  [{sev:8s}]  {inc_type}  —  {summary}")
        timeline_block = (
            "\nINCIDENT TIMELINE (chronological — use this to describe the fault progression):\n"
            + "\n".join(lines) + "\n"
        )

    # ── Raw telemetry trend ────────────────────────────────────────────────────
    event_block = ""
    if recent_events:
        lines = []
        for e in recent_events:
            p     = e.get("payload", {})
            ts    = e.get("ts_ms", 0)
            iso   = datetime.utcfromtimestamp(ts / 1000).strftime("%H:%M:%SZ") if ts else "?"
            etype = e.get("event_type", "?")
            if etype == "telemetry":
                summary = (
                    f"load={p.get('tx_load_pct','?')}%  "
                    f"temp={p.get('tx_temp_c','?')}°C  "
                    f"v={p.get('v_kv','?')}kV"
                )
            elif etype == "temperature_warning":
                summary = (
                    f"temp={p.get('temperature_c','?')}°C  "
                    f"rate={p.get('rate_degC_per_min','?')}°C/min  "
                    f"equip={p.get('equipment_id','?')}"
                )
            else:
                continue
            lines.append(f"  {iso}  [{etype}]  {e.get('feeder_id','')}  {summary}")
        if lines:
            event_block = (
                "\nRAW TELEMETRY READINGS (use to quantify the rate of change):\n"
                + "\n".join(lines) + "\n"
            )

    # ── RAG context ────────────────────────────────────────────────────────────
    if similar:
        rag_lines = []
        for i, s in enumerate(similar, 1):
            meta = s.get("metadata", {})
            rag_lines.append(
                f"\n  Past Incident {i} "
                f"(similarity: {(1 - s.get('distance', 1)) * 100:.0f}%, "
                f"recorded: {meta.get('timestamp_iso', 'unknown')}):\n"
                + "\n".join(f"    {line}" for line in s["text"].split("\n")[:10])
            )
        rag_block = "\nSIMILAR PAST INCIDENTS (for historical comparison):\n" + "".join(rag_lines)
    else:
        rag_block = "\nSIMILAR PAST INCIDENTS: None on record yet — this is the first occurrence."

    user = (
        f"{fault_block}"
        f"{timeline_block}"
        f"{event_block}"
        f"{rag_block}"
        "\n\nWrite a technical incident report with EXACTLY these sections. "
        "Use specific timestamps and values from the data above. "
        "Do NOT use generic placeholder text.\n\n"
        "**1. FAULT ORIGIN & PROGRESSION**\n"
        "Describe how the fault developed over time. Reference the incident timeline — "
        "what fired first, what followed, how readings changed. "
        "If Pattern A, explicitly describe the sustained nature and rate of change.\n\n"
        "**2. ROOT CAUSE**\n"
        "State the most likely root cause in one or two sentences.\n\n"
        "**3. ASSET & CUSTOMER IMPACT**\n"
        "Which transformer/feeder is affected and what is the risk to customers.\n\n"
        "**4. IMMEDIATE ACTIONS (priority order)**\n"
        "Numbered list of operator steps to take right now.\n\n"
        "**5. HISTORICAL COMPARISON**\n"
        "Compare to similar past incidents if available, otherwise state first occurrence.\n\n"
        "**6. PREVENTIVE MEASURES**\n"
        "One or two specific actions to prevent recurrence.\n\n"
        "Keep the total report under 500 words."
    )

    return system, user


# ─── LLM SERVICE ─────────────────────────────────────────────────────────────

class LLMService:
    def __init__(self):
        self.buffer          = EventBuffer(max_age_s=180.0)   # raw telemetry/events
        self.incident_buffer = EventBuffer(max_age_s=180.0)   # CEP incidents — used to build fault timeline
        self.memory          = IncidentMemory(persist_path=CHROMA_PATH)
        self.llm             = OllamaClient(host=OLLAMA_HOST, model=OLLAMA_MODEL)
        self.client: Optional[mqtt.Client] = None

        self._last_summary_t = time.time()
        self._lock = threading.Lock()

    # ── MQTT event handlers ───────────────────────────────────────────────────

    def on_event(self, evt: Dict) -> None:
        """Called for every message on edge/events."""
        self.buffer.add(evt)

        # Check if it's time to generate a summary
        now = time.time()
        if now - self._last_summary_t >= SUMMARY_INTERVAL_S:
            self._last_summary_t = now
            events = self.buffer.drain()
            if events:
                threading.Thread(
                    target=self._generate_summary,
                    args=(events,),
                    daemon=True,
                ).start()

    def on_fault_event(self, fault_event: Dict) -> None:
        """Called for every aggregated FaultEvent on edge/fault_events.

        FaultEvents are the ONLY trigger for LLM reports. Individual incidents
        are buffered (on_incident) so their timeline is available here, but do
        NOT generate their own reports.
        """
        root_cause = fault_event.get("root_cause", "UNKNOWN")
        severity   = fault_event.get("severity", "LOW")
        feeder_id  = fault_event.get("feeder_id", "?")
        print(f"[SERVICE] FaultEvent received: {root_cause} | {severity} | feeder={feeder_id}")

        # Only generate full reports for HIGH and CRITICAL fault events
        if severity not in ("HIGH", "CRITICAL"):
            print(f"[SERVICE] Skipping report for {severity} fault event")
            return

        # Capture full 120s window of raw telemetry + all recent incidents
        recent_events    = self.buffer.peek(last_n_s=120.0)
        recent_incidents = self.incident_buffer.peek(last_n_s=120.0)

        threading.Thread(
            target=self._generate_fault_report,
            args=(fault_event, recent_events, recent_incidents),
            daemon=True,
        ).start()

    def on_incident(self, incident: Dict) -> None:
        """Buffer the incident for fault event timeline context.

        Individual incident reports are NOT generated here — the LLM only
        reports on aggregated FaultEvents (edge/fault_events). Buffering
        incidents lets the fault report describe the full temporal sequence.
        """
        inc_type = incident.get("payload", {}).get("incident_type", "UNKNOWN")
        severity = incident.get("payload", {}).get("severity", "?")
        print(f"[SERVICE] Incident buffered (no individual report): {inc_type} [{severity}]")
        self.incident_buffer.add(incident)

    # ── Fault event report generation ────────────────────────────────────────

    def _generate_fault_report(self, fault_event: Dict, recent_events: List[Dict], recent_incidents: List[Dict]) -> None:
        """Generate an LLM report from an aggregated FaultEvent."""
        root_cause = fault_event.get("root_cause", "UNKNOWN")

        # Step 1 — RAG: find similar past incidents
        # Build a synthetic incident dict compatible with IncidentMemory.find_similar()
        synthetic_incident = {
            "payload": {
                "incident_type": root_cause,
                "severity":      fault_event.get("severity", "HIGH"),
                "summary":       fault_event.get("message", ""),
                "confidence":    fault_event.get("confidence", 0.0),
            }
        }
        print(f"[RAG] Querying ChromaDB for incidents similar to {root_cause}...")
        similar = self.memory.find_similar(synthetic_incident, n=RAG_TOP_K)
        print(f"[RAG] Found {len(similar)} similar past incidents")

        # Step 2 — Build prompt (uses temporal-aware fault event prompt)
        system, user = build_fault_report_prompt(fault_event, similar, recent_events, recent_incidents)

        # Step 3 — Generate with Ollama
        print(f"[OLLAMA] Generating fault report for {root_cause}...")
        report_text = self.llm.chat(system, user, max_tokens=1200)

        if not report_text:
            print("[OLLAMA] Failed to generate report — Ollama unavailable?")
            report_text = (
                f"Report generation failed. "
                f"Fault: {root_cause} ({fault_event.get('severity')}). "
                f"Summary: {fault_event.get('message', 'N/A')}. "
                f"Contributing: {', '.join(fault_event.get('contributing_factors', []))}. "
                f"Please check Ollama service."
            )

        # Step 4 — Store in ChromaDB for future RAG
        doc_id = self.memory.store(synthetic_incident, report_text)

        # Step 5 — Publish to edge/reports
        factors   = fault_event.get("contributing_factors", [])
        payload = {
            "incident_type":     root_cause,
            "confidence":        fault_event.get("confidence", 0.0),
            "incident_summary":  fault_event.get("message", ""),
            "report":            report_text,
            "rag_context_used":  len(similar),
            "similar_incidents": [
                {
                    "type":       s["metadata"].get("incident_type"),
                    "recorded":   s["metadata"].get("timestamp_iso"),
                    "similarity": f"{(1 - s.get('distance', 1)) * 100:.0f}%",
                }
                for s in similar
            ],
            "contributing_factors": factors,
            "stored_as":         doc_id,
            "generated_at":      now_iso(),
            "model":             OLLAMA_MODEL,
            "feeder_id":         fault_event.get("feeder_id", ""),
            "site_id":           SITE_ID,
            "cep_pattern":       "A" if "SUSTAINED" in root_cause else
                                 "B" if "REPEATED" in root_cause or "TRANSIENT" in root_cause else
                                 "CRITICAL",
        }

        print(f"[REPORT] {root_cause} → {report_text[:100]}...")

        if self.client:
            pub(self.client, TOPIC_REPORTS, "llm_report", payload)

    # ── Summary generation ────────────────────────────────────────────────────

    def _generate_summary(self, events: List[Dict]) -> None:
        """Generate a plain-English 10-second narrative and publish it."""

        # Skip if only telemetry — not interesting enough to summarise
        non_telemetry = [e for e in events if e.get("event_type") != "telemetry"]
        if not non_telemetry and len(events) < 5:
            return

        system, user = build_summary_prompt(events)
        narrative = self.llm.chat(system, user, max_tokens=150)

        if not narrative:
            return

        print(f"[SUMMARY] {narrative[:80]}...")

        payload = {
            "narrative":    narrative,
            "event_count":  len(events),
            "window_s":     SUMMARY_INTERVAL_S,
            "non_telemetry_events": [e.get("event_type") for e in non_telemetry],
            "generated_at": now_iso(),
            "model":        OLLAMA_MODEL,
        }

        if self.client:
            pub(self.client, TOPIC_SUMMARIES, "llm_summary", payload)

    # ── Incident report generation ────────────────────────────────────────────

    def _generate_report(self, incident: Dict, recent_events: List[Dict]) -> None:
        """
        Full RAG-based incident report pipeline:
          1. Query ChromaDB for similar past incidents
          2. Build prompt with incident + history + recent events
          3. Call Ollama
          4. Store report in ChromaDB
          5. Publish to edge/reports
        """
        incident_type = incident.get("payload", {}).get("incident_type", "UNKNOWN")

        # Step 1 — Retrieve similar past incidents (RAG)
        print(f"[RAG] Querying ChromaDB for incidents similar to {incident_type}...")
        similar = self.memory.find_similar(incident, n=RAG_TOP_K)
        print(f"[RAG] Found {len(similar)} similar past incidents")

        # Step 2 — Build prompt
        system, user = build_report_prompt(incident, similar, recent_events)

        # Step 3 — Generate report with Ollama
        print(f"[OLLAMA] Generating incident report for {incident_type}...")
        report_text = self.llm.chat(system, user, max_tokens=600)

        if not report_text:
            print("[OLLAMA] Failed to generate report — Ollama unavailable?")
            report_text = (
                f"Report generation failed. "
                f"Incident: {incident_type}. "
                f"Summary: {incident.get('payload', {}).get('summary', 'N/A')}. "
                f"Please check Ollama service."
            )

        # Step 4 — Store in ChromaDB for future RAG
        doc_id = self.memory.store(incident, report_text)

        # Step 5 — Publish to edge/reports
        payload = {
            "incident_type":    incident_type,
            "confidence":       incident.get("payload", {}).get("confidence", 0),
            "incident_summary": incident.get("payload", {}).get("summary", ""),
            "report":           report_text,
            "rag_context_used": len(similar),
            "similar_incidents": [
                {
                    "type":       s["metadata"].get("incident_type"),
                    "recorded":   s["metadata"].get("timestamp_iso"),
                    "similarity": f"{(1 - s.get('distance', 1)) * 100:.0f}%",
                }
                for s in similar
            ],
            "stored_as":        doc_id,
            "generated_at":     now_iso(),
            "model":            OLLAMA_MODEL,
            "feeder_id":        incident.get("feeder_id", ""),
            "transformer_id":   incident.get("transformer_id", ""),
            "site_id":          incident.get("site_id", SITE_ID),
        }

        print(f"[REPORT] {incident_type} → {report_text[:100]}...")

        if self.client:
            pub(self.client, TOPIC_REPORTS, "llm_report", payload)


# ─── MQTT WIRING ─────────────────────────────────────────────────────────────

service = LLMService()


def on_connect(client, userdata, flags, reason_code, properties=None):
    # paho-mqtt v2 passes reason_code (ReasonCode object) instead of rc (int)
    # reason_code == 0 means success in both v1 and v2
    if reason_code == 0:
        client.subscribe(TOPIC_EVENTS)
        client.subscribe(TOPIC_INCIDENTS)
        client.subscribe(TOPIC_FAULT_EVENTS)
        print(f"[MQTT] Connected — subscribed to {TOPIC_EVENTS}, {TOPIC_INCIDENTS}, {TOPIC_FAULT_EVENTS}")
    else:
        print(f"[MQTT] Connection failed with reason_code={reason_code}")


def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode("utf-8"))
    except Exception as e:
        print(f"[MQTT] Failed to parse message: {e}")
        return

    topic = msg.topic

    if topic == TOPIC_EVENTS:
        service.on_event(data)

    elif topic == TOPIC_INCIDENTS:
        service.on_incident(data)

    elif topic == TOPIC_FAULT_EVENTS:
        service.on_fault_event(data)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 60)
    print("LLM Incident Intelligence Service")
    print("=" * 60)
    print(f"MQTT:          {MQTT_HOST}:{MQTT_PORT}")
    print(f"Subscribes:    {TOPIC_EVENTS}, {TOPIC_INCIDENTS}")
    print(f"Publishes:     {TOPIC_SUMMARIES}, {TOPIC_REPORTS}")
    print(f"Ollama:        {OLLAMA_HOST}  model={OLLAMA_MODEL}")
    print(f"ChromaDB:      {CHROMA_PATH}")
    print(f"Summary every: {SUMMARY_INTERVAL_S}s")
    print(f"RAG top-k:     {RAG_TOP_K}")
    print("=" * 60)

    # Model readiness is handled inside OllamaClient.__init__() above.
    # If Ollama is running the model is already pulled or being pulled now.
    # If Ollama is not running the service starts anyway with fallback text.
    if service.llm.is_available():
        print(f"[OLLAMA] Ready")
    else:
        print("[WARNING] Ollama unreachable — reports will use fallback text.")
        print(f"[WARNING] Install Ollama from https://ollama.com then restart this service.")
        print(f"[WARNING] The model '{OLLAMA_MODEL}' will be pulled automatically on next start.")

    # Set up MQTT client — use VERSION2 callback API (paho-mqtt >= 2.0)
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="llm_service")
    client.on_connect = on_connect
    client.on_message = on_message

    service.client = client

    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)

    print("\n[SERVICE] Running. Waiting for events...\n")
    client.loop_forever()


if __name__ == "__main__":
    main()