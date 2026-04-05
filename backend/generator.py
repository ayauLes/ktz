import sqlite3
import time
import random
import math
import os
from datetime import datetime, timezone

DB_PATH = "rawdata.db"
INTERVAL_FILE = "interval.cfg"  # shared config for dynamic interval switching

# ─── Default interval (500ms = 1x, switch to 50ms for 10x highload) ───
DEFAULT_INTERVAL = 0.5


def read_interval():
    """Read interval from shared file (written by server on toggle)."""
    try:
        if os.path.exists(INTERVAL_FILE):
            with open(INTERVAL_FILE, "r") as f:
                val = float(f.read().strip())
                return max(0.01, min(2.0, val))
    except Exception:
        pass
    return DEFAULT_INTERVAL


def get_connection():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
                CREATE TABLE IF NOT EXISTS raw_telemetry (
                                                             id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                                                             lon REAL
                )
                """)

    conn.commit()
    conn.close()


class TelemetryGenerator:
    def __init__(self):
        self.tick = 0

        self.locomotive_id = "kz8a"
        self.speed = 55.0
        self.fuel_level = 100.0
        self.fuel_consumption = 2.5
        self.engine_temp = 78.0
        self.oil_temp = 75.0
        self.brake_pressure = 5.6
        self.voltage = 24.1
        self.current = 120.0
        self.alert_code = None

        self.lat = 51.1605
        self.lon = 71.4704

    def clamp(self, value, low, high):
        return max(low, min(high, value))

    def smooth_change(self, current_value, target_value, step):
        if current_value < target_value:
            current_value += step
            if current_value > target_value:
                current_value = target_value
        elif current_value > target_value:
            current_value -= step
            if current_value < target_value:
                current_value = target_value
        return current_value

    def get_current_phase(self):
        cycle = self.tick % 80

        if 0 <= cycle < 20:
            return "normal"
        elif 20 <= cycle < 35:
            return "overheat"
        elif 35 <= cycle < 50:
            return "recovery"
        elif 50 <= cycle < 65:
            return "low_pressure"
        else:
            return "normal"

    def update_normal(self):
        target_speed = 60 + 10 * math.sin(self.tick / 18)
        self.speed = self.smooth_change(self.speed, target_speed, 1.2)
        self.speed += random.uniform(-0.5, 0.5)
        self.speed = self.clamp(self.speed, 0, 120)

        self.fuel_level -= 0.015
        self.fuel_level = self.clamp(self.fuel_level, 0, 100)

        self.fuel_consumption = 2.0 + self.speed * 0.03 + random.uniform(-0.2, 0.2)
        self.fuel_consumption = self.clamp(self.fuel_consumption, 1.5, 4.5)

        self.current = 90 + self.speed * 0.8 + random.uniform(-5, 5)
        self.current = self.clamp(self.current, 60, 220)

        target_temp = 65 + self.current * 0.15
        self.engine_temp = self.smooth_change(self.engine_temp, target_temp, 0.7)
        self.engine_temp += random.uniform(-0.2, 0.2)
        self.engine_temp = self.clamp(self.engine_temp, 50, 120)

        self.oil_temp = self.smooth_change(self.oil_temp, self.engine_temp - 5, 0.6)
        self.oil_temp += random.uniform(-0.2, 0.2)
        self.oil_temp = self.clamp(self.oil_temp, 50, 115)

        self.brake_pressure += random.uniform(-0.04, 0.04)
        self.brake_pressure = self.clamp(self.brake_pressure, 4.8, 6.2)

        self.voltage = 24 - (self.current / 300)
        self.voltage += random.uniform(-0.05, 0.05)
        self.voltage = self.clamp(self.voltage, 23.2, 24.8)

        self.alert_code = None

    def update_overheat(self):
        self.speed = self.smooth_change(self.speed, 72, 1.0)
        self.speed += random.uniform(-0.2, 0.2)
        self.speed = self.clamp(self.speed, 0, 120)

        self.fuel_level -= 0.02
        self.fuel_level = self.clamp(self.fuel_level, 0, 100)

        self.fuel_consumption = 3.5 + random.uniform(-0.3, 0.3)
        self.fuel_consumption = self.clamp(self.fuel_consumption, 2.8, 5.0)

        self.current = self.smooth_change(self.current, 180, 3.5)
        self.current += random.uniform(-1.0, 1.0)
        self.current = self.clamp(self.current, 60, 250)

        self.engine_temp = self.smooth_change(self.engine_temp, 108, 1.5)
        self.engine_temp += random.uniform(-0.15, 0.15)
        self.engine_temp = self.clamp(self.engine_temp, 50, 125)

        self.oil_temp = self.smooth_change(self.oil_temp, 95, 1.2)
        self.oil_temp += random.uniform(-0.15, 0.15)
        self.oil_temp = self.clamp(self.oil_temp, 50, 120)

        self.brake_pressure += random.uniform(-0.03, 0.03)
        self.brake_pressure = self.clamp(self.brake_pressure, 5.0, 6.0)

        self.voltage = 24 - (self.current / 300)
        self.voltage += random.uniform(-0.05, 0.05)
        self.voltage = self.clamp(self.voltage, 23.0, 24.7)

        self.alert_code = "OVERHEAT" if self.engine_temp >= 100 else None

    def update_recovery(self):
        self.speed = self.smooth_change(self.speed, 45, 1.0)
        self.speed += random.uniform(-0.2, 0.2)
        self.speed = self.clamp(self.speed, 0, 120)

        self.fuel_level -= 0.01
        self.fuel_level = self.clamp(self.fuel_level, 0, 100)

        self.fuel_consumption = 2.2 + random.uniform(-0.2, 0.2)
        self.fuel_consumption = self.clamp(self.fuel_consumption, 1.5, 3.5)

        self.current = self.smooth_change(self.current, 100, 2.5)
        self.current += random.uniform(-1.0, 1.0)
        self.current = self.clamp(self.current, 60, 220)

        self.engine_temp = self.smooth_change(self.engine_temp, 80, 1.0)
        self.engine_temp += random.uniform(-0.15, 0.15)
        self.engine_temp = self.clamp(self.engine_temp, 50, 120)

        self.oil_temp = self.smooth_change(self.oil_temp, 78, 0.8)
        self.oil_temp += random.uniform(-0.15, 0.15)
        self.oil_temp = self.clamp(self.oil_temp, 50, 120)

        self.brake_pressure = self.smooth_change(self.brake_pressure, 5.6, 0.05)
        self.brake_pressure += random.uniform(-0.03, 0.03)
        self.brake_pressure = self.clamp(self.brake_pressure, 5.0, 6.2)

        self.voltage = 24 - (self.current / 300)
        self.voltage += random.uniform(-0.05, 0.05)
        self.voltage = self.clamp(self.voltage, 23.2, 24.8)

        self.alert_code = None

    def update_low_pressure(self):
        self.speed = self.smooth_change(self.speed, 40, 1.0)
        self.speed += random.uniform(-0.2, 0.2)
        self.speed = self.clamp(self.speed, 0, 120)

        self.fuel_level -= 0.01
        self.fuel_level = self.clamp(self.fuel_level, 0, 100)

        self.fuel_consumption = 2.4 + random.uniform(-0.2, 0.2)
        self.fuel_consumption = self.clamp(self.fuel_consumption, 1.5, 4.0)

        self.current = self.smooth_change(self.current, 95, 1.5)
        self.current += random.uniform(-1.0, 1.0)
        self.current = self.clamp(self.current, 60, 220)

        self.engine_temp = self.smooth_change(self.engine_temp, 77, 0.5)
        self.engine_temp += random.uniform(-0.2, 0.2)
        self.engine_temp = self.clamp(self.engine_temp, 50, 120)

        self.oil_temp += random.uniform(-0.2, 0.2)
        self.oil_temp = self.clamp(self.oil_temp, 50, 120)

        self.brake_pressure = self.smooth_change(self.brake_pressure, 3.2, 0.15)
        self.brake_pressure += random.uniform(-0.03, 0.03)
        self.brake_pressure = self.clamp(self.brake_pressure, 2.8, 6.2)

        self.voltage = 24 - (self.current / 300)
        self.voltage += random.uniform(-0.05, 0.05)
        self.voltage = self.clamp(self.voltage, 23.2, 24.8)

        self.alert_code = "LOW_PRESSURE" if self.brake_pressure < 4 else None

    def update_position(self):
        self.lat += 0.0001 * (self.speed / 60)
        self.lon += 0.0001 * (self.speed / 60)

    def generate(self):
        self.tick += 1
        phase = self.get_current_phase()

        if phase == "normal":
            self.update_normal()
        elif phase == "overheat":
            self.update_overheat()
        elif phase == "recovery":
            self.update_recovery()
        elif phase == "low_pressure":
            self.update_low_pressure()

        self.update_position()

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "locomotive_id": self.locomotive_id,
            "speed": round(self.speed, 2),
            "fuel_level": round(self.fuel_level, 2),
            "fuel_consumption": round(self.fuel_consumption, 2),
            "engine_temp": round(self.engine_temp, 2),
            "oil_temp": round(self.oil_temp, 2),
            "brake_pressure": round(self.brake_pressure, 2),
            "voltage": round(self.voltage, 2),
            "current": round(self.current, 2),
            "alert_code": self.alert_code,
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6)
        }


def insert_raw_telemetry(data):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
                INSERT INTO raw_telemetry (
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
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    data["timestamp"],
                    data["locomotive_id"],
                    data["speed"],
                    data["fuel_level"],
                    data["fuel_consumption"],
                    data["engine_temp"],
                    data["oil_temp"],
                    data["brake_pressure"],
                    data["voltage"],
                    data["current"],
                    data["alert_code"],
                    data["lat"],
                    data["lon"]
                ))

    conn.commit()
    conn.close()


def main():
    init_db()
    generator = TelemetryGenerator()

    print("Generator started. Writing telemetry to rawdata.db...")
    print(f"Default interval: {DEFAULT_INTERVAL}s. Switch via {INTERVAL_FILE} or API /set_interval")

    while True:
        interval = read_interval()
        data = generator.generate()
        insert_raw_telemetry(data)
        print(data)
        time.sleep(interval)


if __name__ == "__main__":
    main()