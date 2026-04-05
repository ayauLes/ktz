"""
processor.py — Обработчик телеметрии локомотива (КТЖ) v2

Читает raw_telemetry → вычисляет индекс здоровья → пишет в processed_telemetry.
Поддерживает динамическое переключение интервала через interval.cfg
"""

import json
import logging
import os
import sqlite3
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ktz.processor")

CONFIG_PATH    = os.path.join(os.path.dirname(__file__), "config.json")
INTERVAL_FILE  = "interval.cfg"
DB_PATH        = "rawdata.db"
DEFAULT_INTERVAL = 0.5
CONFIG_RELOAD_INTERVAL = 30


def read_interval():
    """Read interval from shared file (written by server on /set_interval)."""
    try:
        if os.path.exists(INTERVAL_FILE):
            with open(INTERVAL_FILE, "r") as f:
                val = float(f.read().strip())
                return max(0.01, min(2.0, val))
    except Exception:
        pass
    return DEFAULT_INTERVAL


def load_config() -> dict:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        logger.info("Конфигурация загружена из %s", CONFIG_PATH)
        return cfg
    except FileNotFoundError:
        logger.warning("config.json не найден — используются значения по умолчанию")
        return {}


CONFIG = load_config()
_config_last_loaded = time.time()


def maybe_reload_config():
    global CONFIG, _config_last_loaded
    if time.time() - _config_last_loaded > CONFIG_RELOAD_INTERVAL:
        CONFIG = load_config()
        _config_last_loaded = time.time()


def get_connection():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
                CREATE TABLE IF NOT EXISTS processed_telemetry (
                                                                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                   raw_id INTEGER UNIQUE,
                                                                   timestamp TEXT NOT NULL,
                                                                   locomotive_id TEXT,
                                                                   speed REAL, fuel_level REAL, fuel_consumption REAL,
                                                                   engine_temp REAL, oil_temp REAL, brake_pressure REAL,
                                                                   voltage REAL, current REAL, alert_code TEXT,
                                                                   lat REAL, lon REAL,
                                                                   health_index INTEGER, health_status TEXT,
                                                                   health_reasons TEXT, top_factors TEXT,
                                                                   recommendation TEXT, voltage_status TEXT, performance_status TEXT,
                                                                   FOREIGN KEY (raw_id) REFERENCES raw_telemetry(id)
                    )
                """)
    conn.commit()
    conn.close()


def get_last_processed_raw_id() -> int:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT MAX(raw_id) FROM processed_telemetry")
    result = cur.fetchone()[0]
    conn.close()
    return result if result is not None else 0


def get_new_raw_rows(last_raw_id: int) -> list:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
                SELECT id, timestamp, locomotive_id,
                    speed, fuel_level, fuel_consumption,
                    engine_temp, oil_temp, brake_pressure,
                    voltage, current, alert_code, lat, lon
                FROM raw_telemetry
                WHERE id > ?
                ORDER BY id ASC
                """, (last_raw_id,))
    rows = cur.fetchall()
    conn.close()
    return rows


def insert_processed_row(raw_row, health, status, reasons, top_factors,
                         recommendation, voltage_status, performance_status):
    (raw_id, timestamp, locomotive_id, speed, fuel_level, fuel_consumption,
     engine_temp, oil_temp, brake_pressure, voltage, current, alert_code, lat, lon) = raw_row

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
                INSERT OR IGNORE INTO processed_telemetry (
            raw_id, timestamp, locomotive_id,
            speed, fuel_level, fuel_consumption,
            engine_temp, oil_temp, brake_pressure,
            voltage, current, alert_code, lat, lon,
            health_index, health_status, health_reasons,
            top_factors, recommendation, voltage_status, performance_status
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    raw_id, timestamp, locomotive_id,
                    speed, fuel_level, fuel_consumption,
                    engine_temp, oil_temp, brake_pressure,
                    voltage, current, alert_code, lat, lon,
                    health, status,
                    json.dumps(reasons,     ensure_ascii=False),
                    json.dumps(top_factors, ensure_ascii=False),
                    recommendation, voltage_status, performance_status,
                ))
    conn.commit()
    conn.close()


def calculate_voltage_status(voltage: float) -> str:
    vcfg = CONFIG.get("health", {}).get("voltage", {})
    mn  = vcfg.get("min_normal",   23.5)
    mx  = vcfg.get("max_normal",   24.5)
    mnc = vcfg.get("min_critical", 23.2)
    mxc = vcfg.get("max_critical", 24.8)

    if mn <= voltage <= mx:
        return "В норме"
    elif mnc <= voltage <= mxc:
        return "Внимание"
    return "Критично"


def calculate_performance_status(health, engine_temp, brake_pressure, current, alert_code) -> str:
    if alert_code or health < 50 or brake_pressure < 3.5:
        return "Низкая"
    if health < 80 or engine_temp >= 90 or current > 160 or brake_pressure < 4.5:
        return "Сниженная"
    return "Оптимальная"


def build_recommendation(health, reasons, alert_code,
                         engine_temp, brake_pressure, fuel_level, current, voltage) -> str:
    vcfg = CONFIG.get("health", {}).get("voltage", {})
    mn = vcfg.get("min_normal", 23.5)
    mx = vcfg.get("max_normal", 24.5)

    if alert_code == "OVERHEAT" or engine_temp >= 100:
        return "Снизить тяговую нагрузку, уменьшить скорость и проверить систему охлаждения."
    if alert_code == "LOW_PRESSURE" or brake_pressure < 4.0:
        return "Проверить тормозную магистраль и ограничить движение до стабилизации давления."
    if fuel_level <= 20:
        return "Проверить расход топлива и запланировать дозаправку."
    if current > 180:
        return "Проверить электрическую нагрузку и снизить режим работы узлов."
    if voltage < mn or voltage > mx:
        return "Проверить электрическую систему и питание бортовой сети."
    if health < 80:
        return "Требуется повышенный контроль: наблюдать ключевые узлы и тренды параметров."
    return "Состояние стабильное. Продолжать штатный мониторинг."


def calculate_health(row: tuple):
    (_, _, _, speed, fuel_level, fuel_consumption,
     engine_temp, oil_temp, brake_pressure,
     voltage, current, alert_code, lat, lon) = row

    h_cfg  = CONFIG.get("health", {})
    st_cfg = CONFIG.get("status_thresholds", {"normal": 80, "warning": 50})

    health = 100
    factor_impacts = []

    et = h_cfg.get("engine_temp", {})
    if engine_temp >= et.get("critical", 100):
        factor_impacts.append(("Критическая температура двигателя", et.get("penalty_critical", -30)))
    elif engine_temp >= et.get("high", 95):
        factor_impacts.append(("Высокая температура двигателя", et.get("penalty_high", -20)))
    elif engine_temp >= et.get("elevated", 90):
        factor_impacts.append(("Рост температуры двигателя", et.get("penalty_elevated", -10)))

    ot = h_cfg.get("oil_temp", {})
    if oil_temp >= ot.get("critical", 95):
        factor_impacts.append(("Критическая температура масла", ot.get("penalty_critical", -20)))
    elif oil_temp >= ot.get("high", 88):
        factor_impacts.append(("Высокая температура масла", ot.get("penalty_high", -12)))
    elif oil_temp >= ot.get("elevated", 82):
        factor_impacts.append(("Рост температуры масла", ot.get("penalty_elevated", -6)))

    fl = h_cfg.get("fuel_level", {})
    if fuel_level <= fl.get("critical", 10):
        factor_impacts.append(("Критически низкий уровень топлива", fl.get("penalty_critical", -20)))
    elif fuel_level <= fl.get("low", 20):
        factor_impacts.append(("Низкий уровень топлива", fl.get("penalty_low", -10)))

    fc = h_cfg.get("fuel_consumption", {})
    if fuel_consumption >= fc.get("high", 4.0):
        factor_impacts.append(("Повышенный расход топлива", fc.get("penalty_high", -8)))
    elif fuel_consumption >= fc.get("elevated", 3.2):
        factor_impacts.append(("Рост расхода топлива", fc.get("penalty_elevated", -4)))

    bp = h_cfg.get("brake_pressure", {})
    if brake_pressure < bp.get("critical", 3.5):
        factor_impacts.append(("Критическое давление тормозной системы", bp.get("penalty_critical", -30)))
    elif brake_pressure < bp.get("low", 4.0):
        factor_impacts.append(("Низкое давление тормозной системы", bp.get("penalty_low", -20)))
    elif brake_pressure < bp.get("below_norm", 4.5):
        factor_impacts.append(("Давление тормозной системы ниже нормы", bp.get("penalty_below_norm", -10)))

    vc = h_cfg.get("voltage", {})
    if voltage < vc.get("min_critical", 23.2) or voltage > vc.get("max_critical", 24.8):
        factor_impacts.append(("Критическое отклонение напряжения", vc.get("penalty_critical", -15)))
    elif voltage < vc.get("min_normal", 23.5) or voltage > vc.get("max_normal", 24.5):
        factor_impacts.append(("Напряжение вне нормального диапазона", vc.get("penalty_offnorm", -8)))

    cc = h_cfg.get("current", {})
    if current > cc.get("critical", 200):
        factor_impacts.append(("Критическая токовая нагрузка", cc.get("penalty_critical", -15)))
    elif current > cc.get("high", 180):
        factor_impacts.append(("Высокая токовая нагрузка", cc.get("penalty_high", -10)))
    elif current > cc.get("elevated", 160):
        factor_impacts.append(("Повышенная токовая нагрузка", cc.get("penalty_elevated", -5)))

    ap = h_cfg.get("alert_penalty", {})
    if alert_code == "OVERHEAT":
        factor_impacts.append(("Активная авария OVERHEAT", ap.get("OVERHEAT", -20)))
    elif alert_code == "LOW_PRESSURE":
        factor_impacts.append(("Активная авария LOW_PRESSURE", ap.get("LOW_PRESSURE", -20)))
    elif alert_code:
        factor_impacts.append((f"Активный alert: {alert_code}", ap.get("DEFAULT", -10)))

    reasons = []
    for reason, impact in factor_impacts:
        health += impact
        reasons.append(reason)
    health = max(0, min(100, health))

    if health >= st_cfg.get("normal", 80):
        status = "Норма"
    elif health >= st_cfg.get("warning", 50):
        status = "Внимание"
    else:
        status = "Критично"

    voltage_status     = calculate_voltage_status(voltage)
    performance_status = calculate_performance_status(health, engine_temp, brake_pressure, current, alert_code)

    top_factors = [
        {"factor": f, "impact": i}
        for f, i in sorted(factor_impacts, key=lambda x: abs(x[1]), reverse=True)[:5]
    ]

    recommendation = build_recommendation(
        health=health, reasons=reasons, alert_code=alert_code,
        engine_temp=engine_temp, brake_pressure=brake_pressure,
        fuel_level=fuel_level, current=current, voltage=voltage,
    )

    return health, status, reasons, top_factors, recommendation, voltage_status, performance_status


def main():
    init_db()
    logger.info("Processor запущен. Читаю raw_telemetry → processed_telemetry...")
    logger.info("Интервал управляется через %s (по умолчанию %.1fs)", INTERVAL_FILE, DEFAULT_INTERVAL)

    processed_count = 0

    while True:
        maybe_reload_config()
        interval = read_interval()

        last_raw_id = get_last_processed_raw_id()
        new_rows    = get_new_raw_rows(last_raw_id)

        if new_rows:
            for row in new_rows:
                (health, status, reasons, top_factors,
                 recommendation, voltage_status, performance_status) = calculate_health(row)

                insert_processed_row(
                    row, health, status, reasons, top_factors,
                    recommendation, voltage_status, performance_status,
                )
                processed_count += 1

            last = new_rows[-1]
            logger.info(
                "Обработано %d строк (всего %d) | id=%d | health=%d%% [%s] | alert=%s | interval=%.3fs",
                len(new_rows), processed_count, last[0], health, status, last[11] or "—", interval
            )

        time.sleep(interval)


if __name__ == "__main__":
    main()