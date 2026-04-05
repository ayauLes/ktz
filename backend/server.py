"""
server.py — Сервер цифрового двойника локомотива (КТЖ)

Функции:
  - WebSocket с EMA-сглаживанием и дедупликацией
  - Внутренняя очередь событий (asyncio.Queue) — аналог шины событий
  - Обработка highload-всплесков (батчинг без просадки UI)
  - Базовая HTTP-аутентификация для защищённых эндпоинтов
  - Конфигурация порогов из config.json (без перекомпиляции)
  - Структурированные логи (logging)
  - Health-check и метрики сервиса
  - OpenAPI/Swagger — встроен в FastAPI (/docs, /redoc)
"""

import asyncio
import json
import logging
import os
import secrets
import time
from collections import deque
from datetime import datetime, timezone
from typing import Optional

import sqlite3

from fastapi import Depends, FastAPI, HTTPException, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials

# ═══════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ktz.server")

# ═══════════════════════════════════════════════════════
# CONFIG — загружается из файла, без перекомпиляции
# ═══════════════════════════════════════════════════════

# Абсолютный путь к config.json — всегда в той же папке, что server.py
# Работает независимо от рабочей директории при запуске uvicorn
_SERVER_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(_SERVER_DIR, "config.json")

def load_config() -> dict:
    """Загружает конфигурацию из config.json."""
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        logger.info("Конфигурация загружена из: %s", CONFIG_PATH)
        return cfg
    except FileNotFoundError:
        logger.error(
            "config.json НЕ НАЙДЕН по пути: %s\n"
            "  Убедитесь, что config.json находится в той же папке, что и server.py\n"
            "  Используются встроенные значения по умолчанию (admin/ktz2025)",
            CONFIG_PATH
        )
        # Возвращаем дефолтную конфигурацию — сервер продолжает работать
        return {
            "auth": {"username": "admin", "password": "ktz2025"},
            "ema":  {"alpha": 0.3},
            "queue": {"max_size": 500},
        }
    except json.JSONDecodeError as e:
        logger.error("Ошибка разбора config.json: %s — используются значения по умолчанию", e)
        return {
            "auth": {"username": "admin", "password": "ktz2025"},
            "ema":  {"alpha": 0.3},
            "queue": {"max_size": 500},
        }

CONFIG = load_config()

DB_PATH = "rawdata.db"

# ═══════════════════════════════════════════════════════
# FASTAPI APP
# ═══════════════════════════════════════════════════════
app = FastAPI(
    title="КТЖ · Цифровой двойник локомотива",
    version="2.0.0",
    description="""
## API цифрового двойника локомотива

Предоставляет:
- **WebSocket `/ws`** — поток телеметрии в реальном времени (EMA-сглаженные данные)
- **GET `/history`** — история телеметрии
- **GET `/health`** — состояние сервиса
- **GET `/metrics`** — метрики сервиса (требует авторизации)
- **GET `/config`** — текущая конфигурация порогов (требует авторизации)
- **POST `/config/reload`** — перезагрузить конфигурацию без перезапуска (требует авторизации)
    """,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ═══════════════════════════════════════════════════════
# BASIC AUTH
# ═══════════════════════════════════════════════════════
security = HTTPBasic()

def require_auth(credentials: HTTPBasicCredentials = Depends(security)):
    """
    Проверяет Basic Auth для защищённых эндпоинтов.
    Читает актуальные credentials из CONFIG каждый раз —
    поэтому смена пароля через /config/reload применяется немедленно.
    """
    auth_cfg      = CONFIG.get("auth", {})
    expected_user = auth_cfg.get("username", "admin")
    expected_pass = auth_cfg.get("password", "ktz2025")

    # secrets.compare_digest защищает от timing-атак
    ok_user = secrets.compare_digest(
        credentials.username.encode("utf-8"),
        expected_user.encode("utf-8"),
    )
    ok_pass = secrets.compare_digest(
        credentials.password.encode("utf-8"),
        expected_pass.encode("utf-8"),
    )

    if not (ok_user and ok_pass):
        logger.warning(
            "❌ Неудачная авторизация: пользователь='%s' — IP неизвестен (CORS)",
            credentials.username,
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверные учётные данные. Используйте Basic Auth.",
            headers={"WWW-Authenticate": "Basic"},
        )

    logger.info("✅ Авторизован: %s", credentials.username)
    return credentials.username

# ═══════════════════════════════════════════════════════
# SERVICE METRICS
# ═══════════════════════════════════════════════════════
class ServiceMetrics:
    def __init__(self):
        self.start_time        = time.time()
        self.messages_sent     = 0
        self.messages_dropped  = 0
        self.ws_connections    = 0
        self.ws_total          = 0
        self.last_raw_id_seen  = 0
        self.queue_overflows   = 0
        self.highload_batches  = 0

    def uptime_seconds(self) -> float:
        return time.time() - self.start_time

    def to_dict(self) -> dict:
        return {
            "uptime_seconds":    round(self.uptime_seconds(), 1),
            "messages_sent":     self.messages_sent,
            "messages_dropped":  self.messages_dropped,
            "ws_active":         self.ws_connections,
            "ws_total":          self.ws_total,
            "queue_overflows":   self.queue_overflows,
            "highload_batches":  self.highload_batches,
            "last_raw_id_seen":  self.last_raw_id_seen,
        }

METRICS = ServiceMetrics()

# ═══════════════════════════════════════════════════════
# EMA SMOOTHER — Exponential Moving Average
# ═══════════════════════════════════════════════════════
NUMERIC_FIELDS = [
    "speed", "fuel_level", "fuel_consumption",
    "engine_temp", "oil_temp", "brake_pressure",
    "voltage", "current", "health_index",
]

class EMASmoother:
    """
    Сглаживает числовые поля телеметрии методом EMA.
    alpha: 0 = полное сглаживание, 1 = без сглаживания
    """
    def __init__(self, alpha: float = 0.3):
        self.alpha = alpha
        self._state: dict = {}

    def smooth(self, data: dict) -> dict:
        result = dict(data)
        for field in NUMERIC_FIELDS:
            val = data.get(field)
            if val is None:
                continue
            try:
                val = float(val)
            except (TypeError, ValueError):
                continue

            if field not in self._state:
                self._state[field] = val
            else:
                self._state[field] = self.alpha * val + (1 - self.alpha) * self._state[field]

            result[field] = round(self._state[field], 3)
        return result

_ema_alpha = CONFIG.get("ema", {}).get("alpha", 0.3)
smoother = EMASmoother(alpha=_ema_alpha)

# ═══════════════════════════════════════════════════════
# EVENT QUEUE — внутренняя шина событий (asyncio.Queue)
# ═══════════════════════════════════════════════════════
_queue_max = CONFIG.get("queue", {}).get("max_size", 500)
event_queue: asyncio.Queue = asyncio.Queue(maxsize=_queue_max)

# ═══════════════════════════════════════════════════════
# DB HELPERS
# ═══════════════════════════════════════════════════════
def get_connection():
    return sqlite3.connect(DB_PATH)


def get_latest_processed() -> Optional[dict]:
    """Возвращает последнюю обработанную запись телеметрии."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
                SELECT raw_id, timestamp, locomotive_id,
                    speed, fuel_level, fuel_consumption,
                    engine_temp, oil_temp, brake_pressure,
                    voltage, current, alert_code, lat, lon,
                    health_index, health_status, health_reasons,
                    top_factors, recommendation, voltage_status, performance_status
                FROM processed_telemetry
                ORDER BY raw_id DESC LIMIT 1
                """)
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return _row_to_dict(row)


def get_new_rows_since(last_id: int) -> list:
    """Возвращает все необработанные строки после last_id (для highload-батчинга)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
                SELECT raw_id, timestamp, locomotive_id,
                    speed, fuel_level, fuel_consumption,
                    engine_temp, oil_temp, brake_pressure,
                    voltage, current, alert_code, lat, lon,
                    health_index, health_status, health_reasons,
                    top_factors, recommendation, voltage_status, performance_status
                FROM processed_telemetry
                WHERE raw_id > ?
                ORDER BY raw_id ASC
                """, (last_id,))
    rows = cur.fetchall()
    conn.close()
    return [_row_to_dict(r) for r in rows]


def _row_to_dict(row) -> dict:
    return {
        "raw_id":             row[0],
        "timestamp":          row[1],
        "locomotive_id":      row[2],
        "speed":              row[3],
        "fuel_level":         row[4],
        "fuel_consumption":   row[5],
        "engine_temp":        row[6],
        "oil_temp":           row[7],
        "brake_pressure":     row[8],
        "voltage":            row[9],
        "current":            row[10],
        "alert_code":         row[11],
        "lat":                row[12],
        "lon":                row[13],
        "health_index":       row[14],
        "health_status":      row[15],
        "health_reasons":     json.loads(row[16]) if row[16] else [],
        "top_factors":        json.loads(row[17]) if row[17] else [],
        "recommendation":     row[18],
        "voltage_status":     row[19],
        "performance_status": row[20],
    }


def get_history(limit: int = 50) -> list:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
                SELECT timestamp, speed, fuel_level, fuel_consumption,
                    engine_temp, oil_temp, brake_pressure, voltage,
                    current, health_index, health_status, lat, lon
                FROM processed_telemetry
                ORDER BY raw_id DESC LIMIT ?
                """, (limit,))
    rows = cur.fetchall()
    conn.close()
    return [{
        "timestamp":        r[0],  "speed":            r[1],
        "fuel_level":       r[2],  "fuel_consumption": r[3],
        "engine_temp":      r[4],  "oil_temp":         r[5],
        "brake_pressure":   r[6],  "voltage":          r[7],
        "current":          r[8],  "health_index":     r[9],
        "health_status":    r[10], "lat":              r[11],
        "lon":              r[12],
    } for r in rows]

# ═══════════════════════════════════════════════════════
# BACKGROUND PRODUCER — заполняет очередь из БД
# ═══════════════════════════════════════════════════════
async def db_producer():
    """
    Фоновая задача: читает новые строки из БД и кладёт их в очередь.
    Обрабатывает highload-всплески (несколько строк за такт).
    """
    last_id = 0
    logger.info("DB producer запущен")

    while True:
        try:
            new_rows = await asyncio.get_event_loop().run_in_executor(
                None, get_new_rows_since, last_id
            )

            if new_rows:
                if len(new_rows) > 1:
                    METRICS.highload_batches += 1
                    logger.debug("Highload: %d новых строк за такт", len(new_rows))

                # В очередь кладём только последнюю актуальную строку
                # (остальные — статистически устаревшие для UI)
                latest = new_rows[-1]
                last_id = latest["raw_id"]
                METRICS.last_raw_id_seen = last_id

                try:
                    event_queue.put_nowait(latest)
                except asyncio.QueueFull:
                    # Очередь переполнена — сбрасываем старое, кладём новое
                    try:
                        event_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                    event_queue.put_nowait(latest)
                    METRICS.queue_overflows += 1
                    logger.warning("Очередь переполнена — сброс старого события")

        except Exception as e:
            logger.error("DB producer ошибка: %s", e)

        await asyncio.sleep(0.5)

# ═══════════════════════════════════════════════════════
# CONNECTION MANAGER — управляет WS-клиентами
# ═══════════════════════════════════════════════════════
class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)
        METRICS.ws_connections += 1
        METRICS.ws_total += 1
        logger.info("WS клиент подключён. Активных: %d", len(self.active))

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)
        METRICS.ws_connections -= 1
        logger.info("WS клиент отключён. Активных: %d", len(self.active))

    async def broadcast(self, data: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(data)
                METRICS.messages_sent += 1
            except Exception:
                dead.append(ws)
                METRICS.messages_dropped += 1
        for ws in dead:
            self.disconnect(ws)

manager = ConnectionManager()

# ═══════════════════════════════════════════════════════
# BROADCAST LOOP — читает из очереди и рассылает клиентам
# ═══════════════════════════════════════════════════════
_last_sent_raw_id: int = -1

async def broadcast_loop():
    """
    Фоновая задача: читает из event_queue, применяет EMA,
    дедублицирует и рассылает всем WS-клиентам.
    """
    global _last_sent_raw_id
    logger.info("Broadcast loop запущен")

    while True:
        try:
            data = await asyncio.wait_for(event_queue.get(), timeout=1.0)

            # Дедупликация — не отправляем уже отправленные raw_id
            raw_id = data.get("raw_id", -1)
            if raw_id <= _last_sent_raw_id:
                continue
            _last_sent_raw_id = raw_id

            # EMA-сглаживание числовых полей
            smoothed = smoother.smooth(data)

            if manager.active:
                await manager.broadcast(smoothed)

        except asyncio.TimeoutError:
            pass
        except Exception as e:
            logger.error("Broadcast loop ошибка: %s", e)

# ═══════════════════════════════════════════════════════
# STARTUP — запуск фоновых задач
# ═══════════════════════════════════════════════════════
@app.on_event("startup")
async def startup():
    asyncio.create_task(db_producer())
    asyncio.create_task(broadcast_loop())
    auth_user = CONFIG.get("auth", {}).get("username", "admin")
    logger.info("=" * 60)
    logger.info("🚂 Сервер КТЖ запущен v2.0")
    logger.info("📄 Config path : %s", CONFIG_PATH)
    logger.info("🔐 Auth user   : %s", auth_user)
    logger.info("📊 EMA alpha   : %.2f", smoother.alpha)
    logger.info("📖 Swagger UI  : http://127.0.0.1:8000/docs")
    logger.info("=" * 60)

# ═══════════════════════════════════════════════════════
# WEBSOCKET ENDPOINT
# ═══════════════════════════════════════════════════════
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """
    WebSocket поток телеметрии в реальном времени.
    Данные EMA-сглажены и дедублицированы.
    """
    await manager.connect(ws)

    # Сразу отдаём последнее известное состояние
    try:
        latest = await asyncio.get_event_loop().run_in_executor(None, get_latest_processed)
        if latest:
            smoothed = smoother.smooth(latest)
            await ws.send_json(smoothed)
    except Exception:
        pass

    try:
        # Держим соединение живым, принимая ping-сообщения
        while True:
            await ws.receive_text()
    except (WebSocketDisconnect, Exception):
        manager.disconnect(ws)

# ═══════════════════════════════════════════════════════
# REST API
# ═══════════════════════════════════════════════════════
@app.get("/", tags=["System"], summary="Health-check")
def root():
    """Базовый health-check эндпоинт."""
    return {
        "status": "ok",
        "service": "КТЖ Digital Twin",
        "version": "2.0.0",
        "uptime_seconds": round(METRICS.uptime_seconds(), 1),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/health", tags=["System"], summary="Расширенный health-check")
def health():
    """Расширенный health-check с проверкой БД."""
    db_ok = False
    db_rows = 0
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM processed_telemetry")
        db_rows = cur.fetchone()[0]
        conn.close()
        db_ok = True
    except Exception as e:
        logger.error("DB health check failed: %s", e)

    return {
        "status":        "ok" if db_ok else "degraded",
        "db_reachable":  db_ok,
        "db_rows":       db_rows,
        "ws_clients":    METRICS.ws_connections,
        "queue_size":    event_queue.qsize(),
        "uptime_seconds": round(METRICS.uptime_seconds(), 1),
    }


@app.get(
    "/history",
    tags=["Telemetry"],
    summary="История телеметрии",
    response_description="Массив записей телеметрии (от новых к старым)",
)
def history(limit: int = 1800, username: str = Depends(require_auth)):
    """
    Возвращает последние `limit` записей обработанной телеметрии.
    Требует Basic Auth.
    """
    return get_history(limit)


@app.get(
    "/metrics",
    tags=["System"],
    summary="Метрики сервиса (требует авторизации)",
)
def metrics(username: str = Depends(require_auth)):
    """Метрики производительности сервиса. Требует Basic Auth."""
    return {
        **METRICS.to_dict(),
        "ema_alpha":   smoother.alpha,
        "queue_max":   _queue_max,
        "config_path": CONFIG_PATH,
    }


@app.get(
    "/config",
    tags=["Config"],
    summary="Текущая конфигурация (требует авторизации)",
)
def get_config(username: str = Depends(require_auth)):
    """Возвращает текущую конфигурацию порогов и весов. Требует Basic Auth."""
    return CONFIG


@app.post(
    "/config/reload",
    tags=["Config"],
    summary="Перезагрузить конфигурацию без перезапуска (требует авторизации)",
)
def reload_config(username: str = Depends(require_auth)):
    """
    Перечитывает config.json и применяет новые пороги немедленно.
    Позволяет менять конфигурацию без перезапуска сервера.
    """
    global CONFIG
    CONFIG = load_config()
    new_alpha = CONFIG.get("ema", {}).get("alpha", 0.3)
    smoother.alpha = new_alpha
    new_user = CONFIG.get("auth", {}).get("username", "admin")
    logger.info("🔄 Конфигурация перезагружена пользователем: %s | ema_alpha=%.2f | auth_user=%s",
                username, new_alpha, new_user)
    return {
        "status":       "ok",
        "message":      "Конфигурация перезагружена",
        "config_path":  CONFIG_PATH,
        "ema_alpha":    new_alpha,
        "auth_user":    new_user,
    }

# ═══════════════════════════════════════════════════════
# SET INTERVAL ENDPOINT — переключение скорости генерации
# ═══════════════════════════════════════════════════════
from pydantic import BaseModel

class IntervalRequest(BaseModel):
    interval_ms: int  # 500 = 1x (normal), 50 = 10x (highload)

INTERVAL_FILE = os.path.join(_SERVER_DIR, "interval.cfg")

@app.post(
    "/set_interval",
    tags=["Config"],
    summary="Переключить интервал генерации телеметрии",
)
def set_interval(req: IntervalRequest, username: str = Depends(require_auth)):
    """
    Записывает новый интервал (в секундах) в interval.cfg.
    generator.py и processor.py читают его при каждой итерации.
    interval_ms=500 → 1x (штатный режим)
    interval_ms=50  → 10x (highload тест)
    """
    interval_sec = max(0.01, req.interval_ms / 1000.0)
    try:
        with open(INTERVAL_FILE, "w") as f:
            f.write(str(interval_sec))
        mode = "10x HIGHLOAD" if interval_sec < 0.1 else "1x NORMAL"
        logger.info("⚡ Интервал установлен: %.3fs (%s) — пользователь: %s", interval_sec, mode, username)
        return {
            "status": "ok",
            "interval_ms": req.interval_ms,
            "interval_sec": interval_sec,
            "mode": mode,
        }
    except Exception as e:
        logger.error("Ошибка записи interval.cfg: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/get_interval",
    tags=["Config"],
    summary="Получить текущий интервал",
)
def get_interval_endpoint():
    """Возвращает текущий интервал генерации."""
    try:
        if os.path.exists(INTERVAL_FILE):
            with open(INTERVAL_FILE, "r") as f:
                sec = float(f.read().strip())
        else:
            sec = 0.5
        return {
            "interval_sec": sec,
            "interval_ms": int(sec * 1000),
            "mode": "10x HIGHLOAD" if sec < 0.1 else "1x NORMAL",
        }
    except Exception:
        return {"interval_sec": 0.5, "interval_ms": 500, "mode": "1x NORMAL"}