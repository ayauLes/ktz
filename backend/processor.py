import sqlite3
import time
import json

DB_PATH = "rawdata.db"
INTERVAL_SECONDS = 0.5


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
                                                                   speed REAL,
                                                                   fuel_level REAL,
                                                                   fuel_consumption REAL,
                                                                   engine_temp REAL,
                                                                   oil_temp REAL,
                                                                   brake_pressure REAL,
                                                                   voltage REAL,
                                                                   current REAL,
                                                                   alert_code TEXT,
                                                                   lat REAL,
                                                                   lon REAL,
                                                                   health_index INTEGER,
                                                                   health_status TEXT,
                                                                   health_reasons TEXT,
                                                                   top_factors TEXT,
                                                                   recommendation TEXT,
                                                                   voltage_status TEXT,
                                                                   performance_status TEXT,
                                                                   FOREIGN KEY (raw_id) REFERENCES raw_telemetry(id)
                    )
                """)

    conn.commit()
    conn.close()


def calculate_voltage_status(voltage):
    if 23.5 <= voltage <= 24.5:
        return "В норме"
    elif 23.2 <= voltage < 23.5 or 24.5 < voltage <= 24.8:
        return "Внимание"
    return "Критично"


def calculate_performance_status(health, engine_temp, brake_pressure, current, alert_code):
    if alert_code or health < 50 or brake_pressure < 3.5:
        return "Низкая"

    if health < 80 or engine_temp >= 90 or current > 160 or brake_pressure < 4.5:
        return "Сниженная"

    return "Оптимальная"


def build_recommendation(health, reasons, alert_code, engine_temp, brake_pressure, fuel_level, current, voltage):
    if alert_code == "OVERHEAT" or engine_temp >= 100:
        return "Снизить тяговую нагрузку, уменьшить скорость и проверить систему охлаждения."
    if alert_code == "LOW_PRESSURE" or brake_pressure < 4.0:
        return "Проверить тормозную магистраль и ограничить движение до стабилизации давления."
    if fuel_level <= 20:
        return "Проверить расход топлива и запланировать дозаправку."
    if current > 180:
        return "Проверить электрическую нагрузку и снизить режим работы узлов."
    if voltage < 23.5 or voltage > 24.5:
        return "Проверить электрическую систему и питание бортовой сети."
    if health < 80:
        return "Требуется повышенный контроль: наблюдать ключевые узлы и тренды параметров."
    return "Состояние стабильное. Продолжать штатный мониторинг."


def calculate_health(row):
    """
    row format:
    (
        id, timestamp, locomotive_id, speed, fuel_level, fuel_consumption,
        engine_temp, oil_temp, brake_pressure, voltage, current,
        alert_code, lat, lon
    )
    """
    (
        _raw_id,
        _timestamp,
        locomotive_id,
        speed,
        fuel_level,
        fuel_consumption,
        engine_temp,
        oil_temp,
        brake_pressure,
        voltage,
        current,
        alert_code,
        lat,
        lon
    ) = row

    health = 100
    factor_impacts = []

    # Engine temperature
    if engine_temp >= 100:
        factor_impacts.append(("Критическая температура двигателя", -30))
    elif engine_temp >= 95:
        factor_impacts.append(("Высокая температура двигателя", -20))
    elif engine_temp >= 90:
        factor_impacts.append(("Рост температуры двигателя", -10))

    # Oil temperature
    if oil_temp >= 95:
        factor_impacts.append(("Критическая температура масла", -20))
    elif oil_temp >= 88:
        factor_impacts.append(("Высокая температура масла", -12))
    elif oil_temp >= 82:
        factor_impacts.append(("Рост температуры масла", -6))

    # Fuel level
    if fuel_level <= 10:
        factor_impacts.append(("Критически низкий уровень топлива", -20))
    elif fuel_level <= 20:
        factor_impacts.append(("Низкий уровень топлива", -10))

    # Fuel consumption
    if fuel_consumption >= 4.0:
        factor_impacts.append(("Повышенный расход топлива", -8))
    elif fuel_consumption >= 3.2:
        factor_impacts.append(("Рост расхода топлива", -4))

    # Brake pressure
    if brake_pressure < 3.5:
        factor_impacts.append(("Критическое давление тормозной системы", -30))
    elif brake_pressure < 4.0:
        factor_impacts.append(("Низкое давление тормозной системы", -20))
    elif brake_pressure < 4.5:
        factor_impacts.append(("Давление тормозной системы ниже нормы", -10))

    # Voltage
    if voltage < 23.2 or voltage > 24.8:
        factor_impacts.append(("Критическое отклонение напряжения", -15))
    elif voltage < 23.5 or voltage > 24.5:
        factor_impacts.append(("Напряжение вне нормального диапазона", -8))

    # Current
    if current > 200:
        factor_impacts.append(("Критическая токовая нагрузка", -15))
    elif current > 180:
        factor_impacts.append(("Высокая токовая нагрузка", -10))
    elif current > 160:
        factor_impacts.append(("Повышенная токовая нагрузка", -5))

    # Alerts
    if alert_code == "OVERHEAT":
        factor_impacts.append(("Активная авария OVERHEAT", -20))
    elif alert_code == "LOW_PRESSURE":
        factor_impacts.append(("Активная авария LOW_PRESSURE", -20))
    elif alert_code:
        factor_impacts.append((f"Активный alert: {alert_code}", -10))

    # Apply impacts
    reasons = []
    for reason, impact in factor_impacts:
        health += impact
        reasons.append(reason)

    health = max(0, min(100, health))

    if health >= 80:
        status = "Норма"
    elif health >= 50:
        status = "Внимание"
    else:
        status = "Критично"

    voltage_status = calculate_voltage_status(voltage)
    performance_status = calculate_performance_status(
        health, engine_temp, brake_pressure, current, alert_code
    )

    # Top-5 most influential factors
    top_factors_sorted = sorted(factor_impacts, key=lambda x: abs(x[1]), reverse=True)[:5]
    top_factors = [
        {"factor": factor, "impact": impact}
        for factor, impact in top_factors_sorted
    ]

    recommendation = build_recommendation(
        health=health,
        reasons=reasons,
        alert_code=alert_code,
        engine_temp=engine_temp,
        brake_pressure=brake_pressure,
        fuel_level=fuel_level,
        current=current,
        voltage=voltage
    )

    return health, status, reasons, top_factors, recommendation, voltage_status, performance_status


def get_last_processed_raw_id():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT MAX(raw_id) FROM processed_telemetry")
    result = cur.fetchone()[0]

    conn.close()
    return result if result is not None else 0


def get_new_raw_rows(last_raw_id):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
                SELECT
                    id,
                    timestamp,
                    locomotive_id,
                    speed,
                    fuel_level,
                    fuel_consumption,
                    engine_temp,
                    oil_temp,
                    brake_pressure,
                    voltage,
                    current,
                    alert_code,
                    lat,
                    lon
                FROM raw_telemetry
                WHERE id > ?
                ORDER BY id ASC
                """, (last_raw_id,))

    rows = cur.fetchall()
    conn.close()
    return rows


def insert_processed_row(
        raw_row,
        health,
        status,
        reasons,
        top_factors,
        recommendation,
        voltage_status,
        performance_status
):
    (
        raw_id,
        timestamp,
        locomotive_id,
        speed,
        fuel_level,
        fuel_consumption,
        engine_temp,
        oil_temp,
        brake_pressure,
        voltage,
        current,
        alert_code,
        lat,
        lon
    ) = raw_row

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
                INSERT OR IGNORE INTO processed_telemetry (
        raw_id,
        timestamp,
        locomotive_id,
        speed,
        fuel_level,
        fuel_consumption,
        engine_temp,
        oil_temp,
        brake_pressure,
        voltage,
        current,
        alert_code,
        lat,
        lon,
        health_index,
        health_status,
        health_reasons,
        top_factors,
        recommendation,
        voltage_status,
        performance_status
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    raw_id,
                    timestamp,
                    locomotive_id,
                    speed,
                    fuel_level,
                    fuel_consumption,
                    engine_temp,
                    oil_temp,
                    brake_pressure,
                    voltage,
                    current,
                    alert_code,
                    lat,
                    lon,
                    health,
                    status,
                    json.dumps(reasons, ensure_ascii=False),
                    json.dumps(top_factors, ensure_ascii=False),
                    recommendation,
                    voltage_status,
                    performance_status
                ))

    conn.commit()
    conn.close()


def main():
    init_db()
    print("Processor started. Reading raw_telemetry and writing to processed_telemetry...")

    while True:
        last_raw_id = get_last_processed_raw_id()
        new_rows = get_new_raw_rows(last_raw_id)

        if new_rows:
            for row in new_rows:
                (
                    health,
                    status,
                    reasons,
                    top_factors,
                    recommendation,
                    voltage_status,
                    performance_status
                ) = calculate_health(row)

                insert_processed_row(
                    row,
                    health,
                    status,
                    reasons,
                    top_factors,
                    recommendation,
                    voltage_status,
                    performance_status
                )

                print(
                    f"Id={row[0]} | "
                    f"Локомотив={row[2]} | "
                    f"Топливо={row[4]}% | "
                    f"Расход={row[5]} | "
                    f"Здоровье={health}% | "
                    f"Статус={status} | "
                    f"Напряжение={voltage_status} | "
                    f"Производительность={performance_status} | "
                    f"Alert={row[11]}"
                )

        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()