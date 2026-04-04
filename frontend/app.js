const ws = new WebSocket("ws://127.0.0.1:8000/ws");

let maxPoints = 60;

const labels = [];
const healthSeries = [];
const engineTempSeries = [];
const oilTempSeries = [];
const brakePressureSeries = [];
const voltageSeries = [];
const currentSeries = [];
const fuelLevelSeries = [];
const fuelConsumptionSeries = [];

const routePath = [];

// ---------- helpers ----------
function trimSeries() {
    const allSeries = [
        labels,
        healthSeries,
        engineTempSeries,
        oilTempSeries,
        brakePressureSeries,
        voltageSeries,
        currentSeries,
        fuelLevelSeries,
        fuelConsumptionSeries
    ];

    allSeries.forEach(series => {
        while (series.length > maxPoints) {
            series.shift();
        }
    });

    while (routePath.length > maxPoints) {
        routePath.shift();
    }
}

function setWindowSize(size) {
    maxPoints = size;
    trimSeries();
    updateAllCharts();
}

function statusClassFromHealth(health) {
    if (health < 50) return "status-bad";
    if (health < 80) return "status-warn";
    return "status-good";
}

function alertClass(alertCode) {
    if (!alertCode || alertCode === "None") return "alert-normal";
    if (alertCode === "OVERHEAT" || alertCode === "LOW_PRESSURE") return "alert-critical";
    return "alert-warning";
}

function updateText(id, value) {
    document.getElementById(id).textContent = value;
}

function toNumber(value) {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}

function formatPercent(value) {
    const n = toNumber(value);
    return n === null ? "--" : `${n.toFixed(2)}%`;
}

function formatFuelConsumption(value) {
    const n = toNumber(value);
    return n === null ? "0.00 L/min" : `${n.toFixed(2)} L/min`;
}

function formatVoltage(value) {
    const n = toNumber(value);
    return n === null ? "-- V" : `${n.toFixed(2)} V`;
}

function formatCoord(label, value) {
    const n = toNumber(value);
    return n === null ? `${label}: --` : `${label}: ${n.toFixed(6)}`;
}

function makeFallbackRecommendation(data) {
    if (data.recommendation && String(data.recommendation).trim() !== "") {
        return data.recommendation;
    }

    if (data.alert_code === "OVERHEAT") {
        return "Reduce traction load and inspect the cooling system.";
    }
    if (data.alert_code === "LOW_PRESSURE") {
        return "Inspect brake line pressure and reduce movement speed.";
    }

    if (Array.isArray(data.health_reasons) && data.health_reasons.length > 0) {
        return `Check: ${data.health_reasons.slice(0, 2).join(", ")}.`;
    }

    return "No immediate action required. Continue normal monitoring.";
}

// ---------- map ----------
const map = L.map("map").setView([51.1605, 71.4704], 12);

L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "© OpenStreetMap"
}).addTo(map);

const trainMarker = L.marker([51.1605, 71.4704]).addTo(map);
const trainPath = L.polyline(routePath, { weight: 4 }).addTo(map);

// ---------- charts ----------
function makeChart(canvasId, datasets, yMin = undefined, yMax = undefined) {
    return new Chart(document.getElementById(canvasId), {
        type: "line",
        data: {
            labels,
            datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: "index",
                intersect: false
            },
            plugins: {
                legend: {
                    labels: {
                        color: "#eaf2ff"
                    }
                },
                tooltip: {
                    enabled: true
                },
                zoom: {
                    pan: {
                        enabled: true,
                        mode: "x"
                    },
                    zoom: {
                        wheel: { enabled: true },
                        pinch: { enabled: true },
                        mode: "x"
                    }
                }
            },
            scales: {
                x: {
                    ticks: {
                        color: "#95a8c3"
                    },
                    grid: {
                        color: "rgba(255,255,255,0.06)"
                    }
                },
                y: {
                    min: yMin,
                    max: yMax,
                    ticks: {
                        color: "#95a8c3"
                    },
                    grid: {
                        color: "rgba(255,255,255,0.06)"
                    }
                }
            }
        }
    });
}

const healthChart = makeChart(
    "healthChart",
    [
        {
            label: "Health index",
            data: healthSeries,
            borderWidth: 3,
            tension: 0.35
        }
    ],
    0,
    100
);

const tempPressureChart = makeChart(
    "tempPressureChart",
    [
        {
            label: "Engine temp",
            data: engineTempSeries,
            borderWidth: 2,
            tension: 0.35
        },
        {
            label: "Oil temp",
            data: oilTempSeries,
            borderWidth: 2,
            tension: 0.35
        },
        {
            label: "Brake pressure",
            data: brakePressureSeries,
            borderWidth: 2,
            tension: 0.35
        }
    ]
);

const electricalChart = makeChart(
    "electricalChart",
    [
        {
            label: "Voltage",
            data: voltageSeries,
            borderWidth: 2,
            tension: 0.35
        },
        {
            label: "Current",
            data: currentSeries,
            borderWidth: 2,
            tension: 0.35
        }
    ]
);

const fuelChart = makeChart(
    "fuelChart",
    [
        {
            label: "Fuel level",
            data: fuelLevelSeries,
            borderWidth: 2,
            tension: 0.35
        },
        {
            label: "Fuel consumption",
            data: fuelConsumptionSeries,
            borderWidth: 2,
            tension: 0.35
        }
    ]
);

function updateAllCharts() {
    healthChart.update();
    tempPressureChart.update();
    electricalChart.update();
    fuelChart.update();
}

function resetZoomAll() {
    healthChart.resetZoom();
    tempPressureChart.resetZoom();
    electricalChart.resetZoom();
    fuelChart.resetZoom();
}

// ---------- initial history ----------
async function loadHistory() {
    try {
        const res = await fetch("http://127.0.0.1:8000/history?limit=60");
        const history = await res.json();

        history.reverse().forEach(item => {
            labels.push(new Date(item.timestamp).toLocaleTimeString());
            healthSeries.push(toNumber(item.health_index));
            engineTempSeries.push(toNumber(item.engine_temp));
            oilTempSeries.push(toNumber(item.oil_temp));
            brakePressureSeries.push(toNumber(item.brake_pressure));
            voltageSeries.push(toNumber(item.voltage));
            currentSeries.push(toNumber(item.current));
            fuelLevelSeries.push(toNumber(item.fuel_level));
            fuelConsumptionSeries.push(toNumber(item.fuel_consumption));

            const lat = toNumber(item.lat);
            const lon = toNumber(item.lon);

            if (lat !== null && lon !== null) {
                routePath.push([lat, lon]);
            }
        });

        if (routePath.length > 0) {
            trainPath.setLatLngs(routePath);
            trainMarker.setLatLng(routePath[routePath.length - 1]);
            map.setView(routePath[routePath.length - 1], 12);
        }

        updateAllCharts();
    } catch (e) {
        console.error("History load error:", e);
    }
}

// ---------- realtime ----------
ws.onopen = () => {
    updateText("connection_status", "● Live");
};

ws.onclose = () => {
    updateText("connection_status", "● Disconnected");
};

ws.onerror = () => {
    updateText("connection_status", "● Error");
};

ws.onmessage = (event) => {
    const data = JSON.parse(event.data);

    updateText("locomotive_id", data.locomotive_id || "LOCO-001");

    updateText("health_value", `${data.health_index}%`);
    updateText("health_status", data.health_status || "--");

    const healthValueEl = document.getElementById("health_value");
    const healthStatusEl = document.getElementById("health_status");
    const healthClass = statusClassFromHealth(Number(data.health_index));
    healthValueEl.className = `health-value ${healthClass}`;
    healthStatusEl.className = `health-status ${healthClass}`;

    updateText("fuel_value", formatPercent(data.fuel_level));
    updateText("fuel_consumption_value", formatFuelConsumption(data.fuel_consumption));

    updateText("voltage_value", formatVoltage(data.voltage));
    updateText("voltage_status", data.voltage_status || "--");
    updateText("performance_status", data.performance_status || "--");

    const alertEl = document.getElementById("alert_value");
    alertEl.textContent = data.alert_code || "None";
    alertEl.className = `alert-value ${alertClass(data.alert_code)}`;

    updateText("recommendation", makeFallbackRecommendation(data));

    updateText("lat_value", formatCoord("Lat", data.lat));
    updateText("lon_value", formatCoord("Lon", data.lon));

    const factorsList = document.getElementById("top_factors_list");
    factorsList.innerHTML = "";
    if (Array.isArray(data.top_factors) && data.top_factors.length > 0) {
        data.top_factors.forEach(item => {
            const li = document.createElement("li");
            li.textContent = `${item.factor} (${item.impact})`;
            factorsList.appendChild(li);
        });
    } else if (Array.isArray(data.health_reasons) && data.health_reasons.length > 0) {
        data.health_reasons.slice(0, 5).forEach(reason => {
            const li = document.createElement("li");
            li.textContent = reason;
            factorsList.appendChild(li);
        });
    } else {
        const li = document.createElement("li");
        li.textContent = "No significant factors";
        factorsList.appendChild(li);
    }

    labels.push(new Date(data.timestamp).toLocaleTimeString());
    healthSeries.push(toNumber(data.health_index));
    engineTempSeries.push(toNumber(data.engine_temp));
    oilTempSeries.push(toNumber(data.oil_temp));
    brakePressureSeries.push(toNumber(data.brake_pressure));
    voltageSeries.push(toNumber(data.voltage));
    currentSeries.push(toNumber(data.current));
    fuelLevelSeries.push(toNumber(data.fuel_level));
    fuelConsumptionSeries.push(toNumber(data.fuel_consumption));

    const lat = toNumber(data.lat);
    const lon = toNumber(data.lon);

    if (lat !== null && lon !== null) {
        const point = [lat, lon];
        routePath.push(point);
        trainMarker.setLatLng(point);
        trainPath.setLatLngs(routePath);
        map.panTo(point);
    }

    trimSeries();
    updateAllCharts();
};

// ---------- csv export ----------
async function downloadCSV() {
    const res = await fetch("http://127.0.0.1:8000/history?limit=1800");
    const data = await res.json();

    let csv = [
        "timestamp,speed,fuel_level,fuel_consumption,engine_temp,oil_temp,brake_pressure,voltage,current,health_index,lat,lon"
    ];

    data.forEach(d => {
        csv.push(
            `${d.timestamp},${d.speed},${d.fuel_level},${d.fuel_consumption},${d.engine_temp},${d.oil_temp},${d.brake_pressure},${d.voltage},${d.current},${d.health_index},${d.lat},${d.lon}`
        );
    });

    const blob = new Blob([csv.join("\n")], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);

    const a = document.createElement("a");
    a.href = url;
    a.download = "locomotive_report_last_15_minutes.csv";
    a.click();

    URL.revokeObjectURL(url);
}

loadHistory();