from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import asyncio
import json

DB_PATH = "rawdata.db"

app = FastAPI()

# Allow frontend connection (important)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_connection():
    return sqlite3.connect(DB_PATH)


def get_latest_processed():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
                SELECT raw_id, timestamp, speed, fuel_level, engine_temp,
                    brake_pressure, voltage, current, alert_code,
                    health_index, health_status, health_reasons,
                    voltage_status, performance_status
                FROM processed_telemetry
                ORDER BY raw_id DESC
                    LIMIT 1
                """)

    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return {
        "raw_id": row[0],
        "timestamp": row[1],
        "speed": row[2],
        "fuel_level": row[3],
        "engine_temp": row[4],
        "brake_pressure": row[5],
        "voltage": row[6],
        "current": row[7],
        "alert_code": row[8],
        "health_index": row[9],
        "health_status": row[10],
        "health_reasons": json.loads(row[11]) if row[11] else [],
        "voltage_status": row[12],
        "performance_status": row[13]
    }


def get_history(limit=50):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
                SELECT timestamp, speed, fuel_level, engine_temp,
                    brake_pressure, voltage, current,
                    health_index, health_status
                FROM processed_telemetry
                ORDER BY raw_id DESC
                    LIMIT ?
                """, (limit,))

    rows = cur.fetchall()
    conn.close()

    result = []
    for row in rows:
        result.append({
            "timestamp": row[0],
            "speed": row[1],
            "fuel_level": row[2],
            "engine_temp": row[3],
            "brake_pressure": row[4],
            "voltage": row[5],
            "current": row[6],
            "health_index": row[7],
            "health_status": row[8]
        })

    return result


# ---------------------------
# WebSocket (REALTIME)
# ---------------------------
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    print("Client connected")

    try:
        while True:
            data = get_latest_processed()

            if data:
                await ws.send_json(data)

            await asyncio.sleep(0.5)

    except Exception as e:
        print("Client disconnected:", e)


# ---------------------------
# REST API (HISTORY)
# ---------------------------
@app.get("/history")
def history(limit: int = 50):
    return get_history(limit)


# ---------------------------
# HEALTH CHECK
# ---------------------------
@app.get("/")
def root():
    return {"status": "Server is running"}