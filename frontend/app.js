/**
 * app.js — КТЖ Цифровой двойник локомотива v2
 *
 * Новое:
 *  - Login screen: аутентификация через /metrics (Basic Auth)
 *  - Сессия в sessionStorage — при обновлении страницы не нужно входить повторно
 *  - Все запросы к API идут с Authorization header
 *  - EMA-сглаживание на клиенте
 *  - Дедупликация, backoff reconnect, highload RAF throttle
 */

// ═══════════════════════════════════════════════════════
// AUTH SYSTEM
// ═══════════════════════════════════════════════════════
const SESSION_KEY = "ktz_auth";

function getStoredAuth() {
    try {
        const raw = sessionStorage.getItem(SESSION_KEY);
        return raw ? JSON.parse(raw) : null;
    } catch { return null; }
}

function storeAuth(username, password) {
    const encoded = btoa(`${username}:${password}`);
    sessionStorage.setItem(SESSION_KEY, JSON.stringify({ username, encoded }));
}

function clearAuth() {
    sessionStorage.removeItem(SESSION_KEY);
}

function getAuthHeader() {
    const auth = getStoredAuth();
    return auth ? { "Authorization": `Basic ${auth.encoded}` } : {};
}

async function doLogin() {
    const username = document.getElementById("login_username")?.value?.trim();
    const password = document.getElementById("login_password")?.value;
    const errorEl  = document.getElementById("login_error");
    const btn      = document.getElementById("login_btn");
    const btnText  = document.getElementById("login_btn_text");
    const spinner  = document.getElementById("login_spinner");

    if (!username || !password) {
        showLoginError("Введите логин и пароль");
        return;
    }

    // Show spinner
    btn.disabled       = true;
    btnText.style.display = "none";
    spinner.style.display = "inline";
    errorEl.style.display = "none";

    // Remove error styling
    document.getElementById("login_username").classList.remove("input-error");
    document.getElementById("login_password").classList.remove("input-error");

    // Validate against server — use /metrics which requires Basic Auth
    const encoded = btoa(`${username}:${password}`);
    try {
        const res = await fetch(`${BASE_URL}/metrics`, {
            headers: { "Authorization": `Basic ${encoded}` }
        });

        if (res.status === 200) {
            // Success — store session and show dashboard
            storeAuth(username, password);
            showDashboard();
        } else if (res.status === 401) {
            showLoginError("Неверный логин или пароль");
            document.getElementById("login_username").classList.add("input-error");
            document.getElementById("login_password").classList.add("input-error");
        } else {
            showLoginError(`Ошибка сервера: ${res.status}`);
        }
    } catch (e) {
        showLoginError("Сервер недоступен. Проверьте подключение.");
        console.error("[Auth] Fetch error:", e);
    } finally {
        btn.disabled       = false;
        btnText.style.display = "inline";
        spinner.style.display = "none";
    }
}

function showLoginError(msg) {
    const el = document.getElementById("login_error");
    if (!el) return;
    el.textContent    = msg;
    el.style.display  = "block";
    // Re-trigger shake animation
    el.style.animation = "none";
    el.offsetHeight;   // reflow
    el.style.animation = "";
}

function showDashboard() {
    const loginEl = document.getElementById("login_screen");
    const appEl   = document.getElementById("app_shell");
    if (loginEl) loginEl.style.display = "none";
    if (appEl)   appEl.style.display   = "block";
    initDashboard(); // start WS + history after login
}

function doLogout() {
    clearAuth();
    if (ws) { try { ws.close(); } catch (_) {} }
    clearTimeout(_reconnectTimer);
    clearTimeout(_noDataTimer);
    _currentWsState = null; // reset so status renders correctly on next login
    _reconnectAttempt = 0;
    const loginEl = document.getElementById("login_screen");
    const appEl   = document.getElementById("app_shell");
    if (appEl)   { appEl.style.display   = "none"; }
    if (loginEl) { loginEl.style.display = "flex"; }
    // Clear password field for security
    const pwEl = document.getElementById("login_password");
    if (pwEl) pwEl.value = "";
}

// Allow Enter key to submit login
document.addEventListener("DOMContentLoaded", () => {
    // Enter on either field triggers login
    ["login_username", "login_password"].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.addEventListener("keydown", e => { if (e.key === "Enter") doLogin(); });
    });

    // Check if already logged in (session persists across page refresh)
    const existing = getStoredAuth();
    if (existing) {
        // Re-validate session with server
        fetch(`${BASE_URL}/metrics`, {
            headers: { "Authorization": `Basic ${existing.encoded}` }
        }).then(res => {
            if (res.status === 200) {
                showDashboard();
            } else {
                clearAuth(); // Session expired or password changed
            }
        }).catch(() => {
            // Server unreachable — still show login
            clearAuth();
        });
    }
});



// ═══════════════════════════════════════════════════════
// CONSTANTS
// ═══════════════════════════════════════════════════════
const BASE_URL          = "http://127.0.0.1:8000";
const WS_URL            = "ws://127.0.0.1:8000/ws";
const HISTORY_URL       = `${BASE_URL}/history`;
const POINTS_PER_SEC    = 2;
const MAX_STORE_POINTS  = 15 * 60 * POINTS_PER_SEC; // 1800
const MAX_RENDER_POINTS = 300;
const RING_CIRCUMFERENCE = 2 * Math.PI * 90;         // ≈ 565.49

// Reconnect exponential backoff
const BACKOFF_BASE_MS = 1000;
const BACKOFF_MAX_MS  = 30000;
const BACKOFF_FACTOR  = 2;

// Client-side EMA alpha (лёгкий проход поверх серверного сглаживания)
const CLIENT_EMA_ALPHA = 0.6;

let viewSeconds = 900;

// ═══════════════════════════════════════════════════════
// DATA STORE
// ═══════════════════════════════════════════════════════
const store = {
    labels:          [],
    health:          [],
    engineTemp:      [],
    oilTemp:         [],
    brakePressure:   [],
    voltage:         [],
    current:         [],
    fuelLevel:       [],
    fuelConsumption: [],
};

const routePath = [];

// ═══════════════════════════════════════════════════════
// CLIENT-SIDE EMA SMOOTHER
// ═══════════════════════════════════════════════════════
const EMA_FIELDS = [
    "speed", "fuel_level", "fuel_consumption",
    "engine_temp", "oil_temp", "brake_pressure",
    "voltage", "current", "health_index",
];
const _emaState = {};

function emaSmooth(data) {
    const out = { ...data };
    EMA_FIELDS.forEach(field => {
        const v = toNumber(data[field]);
        if (v === null) return;
        if (_emaState[field] === undefined) {
            _emaState[field] = v;
        } else {
            _emaState[field] = CLIENT_EMA_ALPHA * v + (1 - CLIENT_EMA_ALPHA) * _emaState[field];
        }
        out[field] = +_emaState[field].toFixed(3);
    });
    return out;
}

// ═══════════════════════════════════════════════════════
// DEDUPLICATION
// ═══════════════════════════════════════════════════════
let _lastSeenRawId = -1;

function isDuplicate(data) {
    const id = data.raw_id ?? -1;
    if (id !== -1 && id <= _lastSeenRawId) return true;
    if (id !== -1) _lastSeenRawId = id;
    return false;
}

// ═══════════════════════════════════════════════════════
// HIGHLOAD PROTECTION — incoming message queue + RAF throttle
// ═══════════════════════════════════════════════════════
const _msgQueue   = [];
let   _rafPending = false;

function enqueueMessage(data) {
    // Keep only latest — drop stale messages under burst
    _msgQueue.push(data);
    if (_msgQueue.length > 20) _msgQueue.splice(0, _msgQueue.length - 1);
    if (!_rafPending) {
        _rafPending = true;
        requestAnimationFrame(drainQueue);
    }
}

function drainQueue() {
    _rafPending = false;
    if (_msgQueue.length === 0) return;
    // Process only the most recent message per animation frame
    const data = _msgQueue.pop();
    _msgQueue.length = 0;
    processMessage(data);
}

// ═══════════════════════════════════════════════════════
// LIVE CLOCK
// ═══════════════════════════════════════════════════════
function updateClock() {
    const now = new Date();
    const hh = String(now.getHours()).padStart(2, "0");
    const mm = String(now.getMinutes()).padStart(2, "0");
    const ss = String(now.getSeconds()).padStart(2, "0");
    const el = document.getElementById("live_clock");
    if (el) el.textContent = `${hh}:${mm}:${ss}`;
}
setInterval(updateClock, 1000);
updateClock();

// ═══════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════
function toNumber(v) {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
}

function setText(id, val) {
    const el = document.getElementById(id);
    if (el) el.textContent = val;
}

function formatPercent(v)      { const n = toNumber(v); return n === null ? "--"       : `${Math.round(n)}%`; }
function formatConsumption(v)  { const n = toNumber(v); return n === null ? "--"       : `${n.toFixed(2)} Л/мин`; }
function formatVoltage(v)      { const n = toNumber(v); return n === null ? "--"       : `${n.toFixed(2)} В`; }
function formatCoord(label, v) { const n = toNumber(v); return n === null ? `${label}: --` : `${label}: ${n.toFixed(6)}`; }

function healthColorClass(h) {
    if (h < 50) return "bad";
    if (h < 80) return "warn";
    return "good";
}

function alertClass(code) {
    if (!code || code === "None" || code === "null") return "ok";
    if (code === "OVERHEAT" || code === "LOW_PRESSURE") return "critical";
    return "warning";
}

function getRecommendation(data) {
    if (data.recommendation && String(data.recommendation).trim()) return data.recommendation;
    if (data.alert_code === "OVERHEAT")     return "Снизить тяговую нагрузку и проверить систему охлаждения.";
    if (data.alert_code === "LOW_PRESSURE") return "Проверить тормозную магистраль и ограничить скорость.";
    if (Array.isArray(data.health_reasons) && data.health_reasons.length)
        return `Проверить: ${data.health_reasons.slice(0, 2).join(", ")}.`;
    return "Состояние стабильное. Продолжать штатный мониторинг.";
}

// ═══════════════════════════════════════════════════════
// HEALTH RING (SVG animated arc)
// ═══════════════════════════════════════════════════════
function updateHealthRing(value) {
    const arc  = document.getElementById("ring_arc");
    const glow = document.getElementById("ring_glow");
    if (!arc || !glow) return;

    const pct    = Math.max(0, Math.min(100, value));
    const offset = RING_CIRCUMFERENCE * (1 - pct / 100);

    const cls = healthColorClass(pct);
    const colorMap = { good: "#00ff9d", warn: "#ffcc00", bad: "#ff3b3b" };
    const color = colorMap[cls];

    arc.style.strokeDashoffset = offset;
    arc.style.stroke = color;
    glow.style.strokeDashoffset = offset;
    glow.style.stroke = color;
}

// ═══════════════════════════════════════════════════════
// STORE
// ═══════════════════════════════════════════════════════
function pushPoint(timestamp, data) {
    const t = new Date(timestamp).toLocaleTimeString("ru-RU");
    store.labels.push(t);
    store.health.push(toNumber(data.health_index));
    store.engineTemp.push(toNumber(data.engine_temp));
    store.oilTemp.push(toNumber(data.oil_temp));
    store.brakePressure.push(toNumber(data.brake_pressure));
    store.voltage.push(toNumber(data.voltage));
    store.current.push(toNumber(data.current));
    store.fuelLevel.push(toNumber(data.fuel_level));
    store.fuelConsumption.push(toNumber(data.fuel_consumption));

    if (store.labels.length > MAX_STORE_POINTS) {
        Object.keys(store).forEach(k => store[k].shift());
    }
}

function decimate(arr, step) {
    if (step <= 1) return arr;
    return arr.filter((_, i) => i % step === 0);
}

function getViewSlice(series) {
    const pts   = Math.min(viewSeconds * POINTS_PER_SEC, series.length);
    const slice = series.slice(series.length - pts);
    const step  = Math.max(1, Math.floor(slice.length / MAX_RENDER_POINTS));
    return decimate(slice, step);
}

// ═══════════════════════════════════════════════════════
// CHART CONFIG FACTORY
// ═══════════════════════════════════════════════════════
const CHART_DEFAULTS = {
    type: "line",
    options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
            legend: {
                display: true,
                position: "top",
                labels: {
                    color: "rgba(200,223,245,0.55)",
                    font: { family: "'Space Mono', monospace", size: 9 },
                    boxWidth: 14,
                    padding: 10,
                }
            },
            tooltip: {
                backgroundColor: "rgba(6,14,26,0.95)",
                borderColor: "rgba(0,229,255,0.2)",
                borderWidth: 1,
                titleColor: "rgba(200,223,245,0.6)",
                bodyColor: "#e8f4ff",
                titleFont: { family: "'Space Mono', monospace", size: 10 },
                bodyFont: { family: "'Space Mono', monospace", size: 10 },
            },
            zoom: {
                zoom: { wheel: { enabled: true }, pinch: { enabled: true }, mode: "x" },
                pan: { enabled: true, mode: "x" },
            }
        },
        scales: {
            x: {
                ticks: {
                    color: "rgba(200,223,245,0.3)",
                    font: { family: "'Space Mono', monospace", size: 8 },
                    maxRotation: 0,
                    maxTicksLimit: 6,
                },
                grid: { color: "rgba(255,255,255,0.04)" },
            },
            y: {
                ticks: {
                    color: "rgba(200,223,245,0.35)",
                    font: { family: "'Space Mono', monospace", size: 8 },
                },
                grid: { color: "rgba(255,255,255,0.04)" },
            }
        }
    }
};

function makeChart(canvasId, datasets, yMin, yMax) {
    const ctx = document.getElementById(canvasId).getContext("2d");
    const opts = JSON.parse(JSON.stringify(CHART_DEFAULTS));
    if (yMin !== undefined) opts.options.scales.y.min = yMin;
    if (yMax !== undefined) opts.options.scales.y.max = yMax;
    return new Chart(ctx, { ...opts, data: { labels: [], datasets } });
}

function ds(label, color, fill = false) {
    return {
        label,
        data: [],
        borderColor: color,
        backgroundColor: fill ? color.replace(")", ", 0.08)").replace("rgb", "rgba") : "transparent",
        borderWidth: 1.5,
        tension: 0.35,
        fill,
        pointRadius: 0,
        pointHoverRadius: 3,
    };
}

// ═══════════════════════════════════════════════════════
// CHARTS
// ═══════════════════════════════════════════════════════
const healthChart = makeChart("healthChart",
    [ds("Индекс здоровья", "#00ff9d", true)],
    0, 100
);

const tempPressureChart = makeChart("tempPressureChart", [
    ds("Темп. двигателя (°C)", "#ff6b6b"),
    ds("Темп. масла (°C)", "#ffa94d"),
    ds("Давление тормозов (бар)", "#74c0fc"),
]);

const electricalChart = makeChart("electricalChart", [
    ds("Напряжение (В)", "#00e5ff"),
    ds("Ток (А)", "#fb923c"),
]);

const fuelChart = makeChart("fuelChart", [
    ds("Уровень топлива (%)", "#00ff9d", false),
    ds("Расход (Л/мин)", "#f472b6"),
]);

function syncChartsFromStore() {
    const vL  = getViewSlice(store.labels);
    const vH  = getViewSlice(store.health);
    const vET = getViewSlice(store.engineTemp);
    const vOT = getViewSlice(store.oilTemp);
    const vBP = getViewSlice(store.brakePressure);
    const vV  = getViewSlice(store.voltage);
    const vC  = getViewSlice(store.current);
    const vFL = getViewSlice(store.fuelLevel);
    const vFC = getViewSlice(store.fuelConsumption);

    [healthChart, tempPressureChart, electricalChart, fuelChart].forEach(ch => {
        ch.data.labels.splice(0, 9999, ...vL);
    });

    healthChart.data.datasets[0].data.splice(0, 9999, ...vH);

    tempPressureChart.data.datasets[0].data.splice(0, 9999, ...vET);
    tempPressureChart.data.datasets[1].data.splice(0, 9999, ...vOT);
    tempPressureChart.data.datasets[2].data.splice(0, 9999, ...vBP);

    electricalChart.data.datasets[0].data.splice(0, 9999, ...vV);
    electricalChart.data.datasets[1].data.splice(0, 9999, ...vC);

    fuelChart.data.datasets[0].data.splice(0, 9999, ...vFL);
    fuelChart.data.datasets[1].data.splice(0, 9999, ...vFC);
}

function updateAllCharts() {
    healthChart.update("none");
    tempPressureChart.update("none");
    electricalChart.update("none");
    fuelChart.update("none");
}

function resetZoomAll() {
    [healthChart, tempPressureChart, electricalChart, fuelChart].forEach(c => c.resetZoom());
}

// ═══════════════════════════════════════════════════════
// TIME WINDOW
// ═══════════════════════════════════════════════════════
function setTimeWindow(seconds, btnId) {
    viewSeconds = seconds;
    document.querySelectorAll(".time-btn").forEach(b => b.classList.remove("active-btn"));
    const btn = document.getElementById(btnId);
    if (btn) btn.classList.add("active-btn");
    syncChartsFromStore();
    updateAllCharts();
}

// ═══════════════════════════════════════════════════════
// MAP — initialized lazily after login to avoid broken container size
// ═══════════════════════════════════════════════════════
let map, trainMarker, trainPath;

function initMap() {
    if (map) return; // already initialized
    map = L.map("map", { zoomControl: true }).setView([51.1605, 71.4704], 13);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
        attribution: "© OpenStreetMap",
        maxZoom: 18,
    }).addTo(map);

    const trainIcon = L.divIcon({
        className: "",
        html: `<div style="
            width:16px;height:16px;border-radius:50%;
            background:#00ff9d;
            box-shadow:0 0 0 3px rgba(0,255,157,0.25), 0 0 20px rgba(0,255,157,0.5);
            border:2px solid rgba(0,0,0,0.6);
        "></div>`,
        iconSize: [16, 16],
        iconAnchor: [8, 8],
    });

    trainMarker = L.marker([51.1605, 71.4704], { icon: trainIcon }).addTo(map);
    trainPath   = L.polyline([], {
        color: "#00e5ff",
        weight: 3,
        opacity: 0.7,
        dashArray: "6 3",
    }).addTo(map);

    // Force Leaflet to recalculate size after the container becomes visible
    setTimeout(() => map.invalidateSize(), 150);
}

// ═══════════════════════════════════════════════════════
// UI UPDATE
// ═══════════════════════════════════════════════════════
function updateUI(data) {
    const healthNum = toNumber(data.health_index) ?? 0;
    const cls       = healthColorClass(healthNum);
    const colorMap  = { good: "var(--good)", warn: "var(--warn)", bad: "var(--bad)" };
    const color     = colorMap[cls];

    // Health ring + numbers
    updateHealthRing(healthNum);
    const hvEl = document.getElementById("health_value");
    if (hvEl) { hvEl.textContent = Math.round(healthNum); hvEl.style.color = color; }

    const hsBadge = document.getElementById("health_status");
    if (hsBadge) {
        hsBadge.textContent = data.health_status || "--";
        hsBadge.className = `health-status-badge badge-${cls}`;
    }

    // Locomotive ID
    setText("locomotive_id", data.locomotive_id || "kz8a");

    // KPI cards
    setText("fuel_value", formatPercent(data.fuel_level));
    setText("fuel_consumption_value", formatConsumption(data.fuel_consumption));
    setText("voltage_value", formatVoltage(data.voltage));
    setText("voltage_status", data.voltage_status || "--");
    setText("performance_status", data.performance_status || "--");

    // Fuel level bar
    const fuelBar = document.getElementById("fuel_bar");
    if (fuelBar) {
        const fl = toNumber(data.fuel_level) ?? 0;
        fuelBar.style.width = `${fl}%`;
        fuelBar.style.background =
            fl < 20 ? "linear-gradient(90deg,#ff3b3b,rgba(255,59,59,0.4))" :
                fl < 40 ? "linear-gradient(90deg,#ffcc00,rgba(255,204,0,0.4))" :
                    "linear-gradient(90deg,#00ff9d,rgba(0,255,157,0.4))";
    }

    // Alert
    const alertEl  = document.getElementById("alert_value");
    const alertBlk = document.getElementById("alert_block");
    if (alertEl && alertBlk) {
        const code = data.alert_code && data.alert_code !== "null" ? data.alert_code : null;
        const ac   = alertClass(code);

        alertEl.textContent = code || "НЕТ";
        alertEl.className   = `alert-code${ac === "critical" ? " is-critical" : ac === "warning" ? " is-warning" : ""}`;
        alertBlk.className  = `alert-block${code ? " is-alert" : ""}`;
    }

    // Recommendation
    setText("recommendation", getRecommendation(data));

    // Top factors
    const factorsList = document.getElementById("top_factors_list");
    if (factorsList) {
        factorsList.innerHTML = "";
        const factors = Array.isArray(data.top_factors) && data.top_factors.length
            ? data.top_factors
            : null;

        if (factors) {
            const maxImpact = Math.max(...factors.map(f => Math.abs(f.impact)));
            factors.forEach(item => {
                const pct  = maxImpact > 0 ? (Math.abs(item.impact) / maxImpact * 100) : 0;
                const sign = item.impact >= 0 ? "+" : "";
                const isPos = item.impact >= 0;
                const div  = document.createElement("div");
                div.className = "factor-item";
                div.innerHTML = `
                    <div class="factor-bar-wrap">
                        <div class="factor-bar" style="width:${pct}%;background:${isPos ? 'linear-gradient(90deg,#00ff9d,rgba(0,255,157,0.3))' : 'linear-gradient(90deg,#ff3b3b,rgba(255,59,59,0.3))'}"></div>
                    </div>
                    <div class="factor-name">${item.factor}</div>
                    <div class="factor-score${isPos ? " positive" : ""}">${sign}${item.impact}</div>
                `;
                factorsList.appendChild(div);
            });
        } else {
            factorsList.innerHTML = '<div class="factor-item placeholder"><div class="factor-name">Нет значимых факторов</div></div>';
        }
    }

    // Coordinates
    setText("lat_value", `ШИР: ${toNumber(data.lat)?.toFixed(6) ?? "--"}`);
    setText("lon_value", `ДОЛ: ${toNumber(data.lon)?.toFixed(6) ?? "--"}`);
}

// ═══════════════════════════════════════════════════════
// THEME TOGGLE
// ═══════════════════════════════════════════════════════
const THEME_KEY = 'ktz_theme';

function applyTheme(theme) {
    document.documentElement.setAttribute('data-theme', theme);
    const icon = document.getElementById('theme_icon');
    if (icon) icon.textContent = theme === 'light' ? '🌙' : '☀';
    localStorage.setItem(THEME_KEY, theme);
}

function toggleTheme() {
    const cur = document.documentElement.getAttribute('data-theme') || 'dark';
    applyTheme(cur === 'dark' ? 'light' : 'dark');
    updateAllChartsTheme();
}

(function() {
    const saved = localStorage.getItem(THEME_KEY) || 'dark';
    applyTheme(saved);
})();

// ═══════════════════════════════════════════════════════
// HIGHLOAD TOGGLE
// ═══════════════════════════════════════════════════════
let _isHighload = false;

function toggleHighload(enabled) {
    _isHighload = enabled;
    const label = document.getElementById('highload_label');
    if (label) label.textContent = enabled ? '10x' : '1x';
    fetch(`${BASE_URL}/set_interval`, {
        method: 'POST',
        headers: { ...getAuthHeader(), 'Content-Type': 'application/json' },
        body: JSON.stringify({ interval_ms: enabled ? 50 : 500 })
    }).catch(() => {});
}


// ═══════════════════════════════════════════════════════
// WEBSOCKET — exponential backoff reconnect
// ═══════════════════════════════════════════════════════
let ws;
let _reconnectAttempt  = 0;
let _reconnectTimer    = null;
let _noDataTimer       = null;
let _currentWsState    = null; // track current state to avoid redundant DOM writes
const NO_DATA_TIMEOUT  = 3000; // показать «нет данных» через 3 сек тишины

function getBackoffMs() {
    const ms = BACKOFF_BASE_MS * Math.pow(BACKOFF_FACTOR, _reconnectAttempt);
    return Math.min(ms, BACKOFF_MAX_MS);
}

function setWsStatus(state, extra) {
    // Only update DOM if state actually changed (prevents flicker on every message)
    const key = state + (extra || "");
    if (_currentWsState === key) return;
    _currentWsState = key;

    const el  = document.getElementById("connection_status");
    const dot = `<span class="status-dot"></span>`;
    if (!el) return;

    if (state === "connected") {
        el.innerHTML  = `${dot} В реальном времени`;
        el.className  = "ticker-value ws-status ws-connected";
    } else if (state === "reconnecting") {
        const secs = Math.round((extra || 0) / 1000);
        el.innerHTML  = `${dot} Переподключение через ${secs}с...`;
        el.className  = "ticker-value ws-status ws-error";
    } else if (state === "nodata") {
        el.innerHTML  = `${dot} Нет данных`;
        el.className  = "ticker-value ws-status ws-error";
    } else {
        el.innerHTML  = `${dot} Отключён`;
        el.className  = "ticker-value ws-status ws-error";
    }
}

function resetNoDataTimer() {
    clearTimeout(_noDataTimer);
    _noDataTimer = setTimeout(() => setWsStatus("nodata"), NO_DATA_TIMEOUT);
}

function connectWS() {
    if (ws) {
        try { ws.close(); } catch (_) {}
    }

    ws = new WebSocket(WS_URL);

    ws.onopen = () => {
        _reconnectAttempt = 0;
        setWsStatus("connected");
        resetNoDataTimer();
        console.log("[WS] Connected");
    };

    ws.onclose = () => {
        clearTimeout(_noDataTimer);
        const delay = getBackoffMs();
        _reconnectAttempt++;
        setWsStatus("reconnecting", delay);
        console.warn(`[WS] Closed — retry #${_reconnectAttempt} in ${delay}ms`);
        clearTimeout(_reconnectTimer);
        _reconnectTimer = setTimeout(connectWS, delay);
    };

    ws.onerror = (e) => {
        console.error("[WS] Error:", e);
    };

    ws.onmessage = (event) => {
        resetNoDataTimer();

        let data;
        try { data = JSON.parse(event.data); }
        catch (e) { console.error("[WS] JSON parse error:", e); return; }

        // Deduplication
        if (isDuplicate(data)) return;

        // Client-side EMA smoothing
        const smoothed = emaSmooth(data);

        // Highload protection — queue + RAF
        enqueueMessage(smoothed);
    };
}

// ═══════════════════════════════════════════════════════
// MESSAGE PROCESSOR (called from RAF drain)
// ═══════════════════════════════════════════════════════
// ═══════════════════════════════════════════════════════
// SPEED ARC
// ═══════════════════════════════════════════════════════
const SPEED_MAX = 120;
const SPEED_CIRCUMFERENCE = 2 * Math.PI * 55;
const SPEED_ARC_FRACTION  = 0.75;

function updateSpeedArc(speed) {
    const arc = document.getElementById("speed_arc");
    const val = document.getElementById("speed_dial_val");
    if (!arc) return;
    const pct    = Math.max(0, Math.min(1, speed / SPEED_MAX));
    const filled = SPEED_ARC_FRACTION * SPEED_CIRCUMFERENCE * pct;
    const empty  = SPEED_CIRCUMFERENCE - filled;
    arc.setAttribute("stroke-dasharray", `${filled.toFixed(2)} ${(SPEED_CIRCUMFERENCE - filled + (SPEED_CIRCUMFERENCE * 0.25)).toFixed(2)}`);
    const color = speed > 80 ? "#ff3b3b" : speed > 60 ? "#ffcc00" : "#00e5ff";
    arc.style.stroke = color;
    if (val) val.textContent = Math.round(speed);
}

// ═══════════════════════════════════════════════════════
// ALERT HISTORY
// ═══════════════════════════════════════════════════════
const _alertHistory = [];
const MAX_ALERT_HIST = 8;

function pushAlertHistory(code, timestamp) {
    if (!code || code === "null" || code === "None") return;
    const last = _alertHistory[0];
    if (last && last.code === code) return;
    _alertHistory.unshift({ code, time: new Date(timestamp).toLocaleTimeString("ru-RU") });
    if (_alertHistory.length > MAX_ALERT_HIST) _alertHistory.pop();
    renderAlertHistory();
}

function renderAlertHistory() {
    const el = document.getElementById("alert_hist_list");
    if (!el) return;
    if (_alertHistory.length === 0) {
        el.innerHTML = '<div class="alert-hist-empty">Алертов нет</div>';
        return;
    }
    el.innerHTML = _alertHistory.map(a => `
        <div class="alert-hist-item">
            <span class="alert-hist-code">${a.code}</span>
            <span class="alert-hist-time">${a.time}</span>
        </div>
    `).join("");
}

// ═══════════════════════════════════════════════════════
// DISTANCE TRACKING
// ═══════════════════════════════════════════════════════
let _totalDistKm = 0;
let _lastLatLon   = null;

function updateDistance(lat, lon) {
    if (_lastLatLon) {
        const [lt, ln] = _lastLatLon;
        const R = 6371;
        const dLat = (lat - lt) * Math.PI / 180;
        const dLon = (lon - ln) * Math.PI / 180;
        const a = Math.sin(dLat/2)**2 + Math.cos(lt*Math.PI/180)*Math.cos(lat*Math.PI/180)*Math.sin(dLon/2)**2;
        _totalDistKm += R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    }
    _lastLatLon = [lat, lon];
    const el = document.getElementById("dist_value");
    if (el) el.textContent = `ДИСТ: ${_totalDistKm.toFixed(2)} КМ`;
}

// ═══════════════════════════════════════════════════════
// PANELS UPDATE
// ═══════════════════════════════════════════════════════
function updatePanels(data) {
    const speed = toNumber(data.speed) ?? 0;
    const fuel  = toNumber(data.fuel_level) ?? 0;
    const cons  = toNumber(data.fuel_consumption) ?? 0;
    const curr  = toNumber(data.current) ?? 0;
    const vol   = toNumber(data.voltage) ?? 0;
    const eng   = toNumber(data.engine_temp) ?? 0;
    const oil   = toNumber(data.oil_temp) ?? 0;
    const brk   = toNumber(data.brake_pressure) ?? 0;

    updateSpeedArc(speed);

    // Fuel/Energy bars
    setText("fe_fuel_level", `${Math.round(fuel)}%`);
    setText("fe_consumption", cons.toFixed(2));
    setText("fe_current", `${Math.round(curr)} А`);
    const fuelEl = document.getElementById("fe_fuel_bar");
    if (fuelEl) {
        fuelEl.style.width = `${fuel}%`;
        fuelEl.style.background = fuel < 20 ? "linear-gradient(90deg,#ff3b3b,rgba(255,59,59,0.4))" :
            fuel < 40 ? "linear-gradient(90deg,#ffcc00,rgba(255,204,0,0.4))" :
                "linear-gradient(90deg,#00ff9d,rgba(0,255,157,0.4))";
    }
    const consEl = document.getElementById("fe_cons_bar");
    if (consEl) consEl.style.width = `${Math.min(100, cons / 5.0 * 100).toFixed(0)}%`;
    const currEl = document.getElementById("fe_curr_bar");
    if (currEl) currEl.style.width = `${Math.min(100, curr / 220 * 100).toFixed(0)}%`;

    // Pressure / Temp
    const engColor = eng >= 100 ? "var(--bad)" : eng >= 90 ? "var(--warn)" : "var(--good)";
    const oilColor = oil >= 95  ? "var(--bad)" : oil >= 82 ? "var(--warn)" : "var(--good)";
    const brkColor = brk < 3.5  ? "var(--bad)" : brk < 4.5 ? "var(--warn)" : "var(--good)";
    const ptEng = document.getElementById("pt_engine_temp");
    const ptOil = document.getElementById("pt_oil_temp");
    const ptBrk = document.getElementById("pt_brake");
    if (ptEng) { ptEng.textContent = `${eng.toFixed(1)} °C`;    ptEng.style.color = engColor; }
    if (ptOil) { ptOil.textContent = `${oil.toFixed(1)} °C`;    ptOil.style.color = oilColor; }
    if (ptBrk) { ptBrk.textContent = `${brk.toFixed(2)} бар`;  ptBrk.style.color = brkColor; }

    // Electrical
    const volColor = (vol >= 23.5 && vol <= 24.5) ? "var(--good)" : (vol >= 23.2 && vol <= 24.8) ? "var(--warn)" : "var(--bad)";
    const elVol = document.getElementById("elec_voltage");
    const elCur = document.getElementById("elec_current");
    const elSta = document.getElementById("elec_status");
    const elLoa = document.getElementById("elec_load");
    if (elVol) { elVol.textContent = vol.toFixed(2); elVol.style.color = volColor; }
    if (elCur) { elCur.textContent = Math.round(curr); elCur.style.color = curr > 200 ? "var(--bad)" : curr > 160 ? "var(--warn)" : "var(--good)"; }
    if (elSta) { elSta.textContent = data.voltage_status || "--"; elSta.style.color = volColor; }
    if (elLoa) {
        const pct = Math.min(100, Math.round(curr / 220 * 100));
        elLoa.textContent = pct;
        elLoa.style.color = pct > 90 ? "var(--bad)" : pct > 70 ? "var(--warn)" : "var(--good)";
    }

    // Alert history
    const code = data.alert_code && data.alert_code !== "null" ? data.alert_code : null;
    pushAlertHistory(code, data.timestamp);

    // Distance
    const lat = toNumber(data.lat);
    const lon = toNumber(data.lon);
    if (lat !== null && lon !== null) updateDistance(lat, lon);
}

// ═══════════════════════════════════════════════════════
// CHART THEME REFRESH
// ═══════════════════════════════════════════════════════
function updateAllChartsTheme() {
    const isLight = document.documentElement.getAttribute('data-theme') === 'light';
    const tickColor   = isLight ? 'rgba(45,74,110,0.4)'   : 'rgba(200,223,245,0.3)';
    const gridColor   = isLight ? 'rgba(0,0,0,0.05)'      : 'rgba(255,255,255,0.04)';
    const legendColor = isLight ? 'rgba(45,74,110,0.7)'   : 'rgba(200,223,245,0.55)';
    const tooltipBg   = isLight ? 'rgba(255,255,255,0.97)': 'rgba(6,14,26,0.95)';
    const tooltipTitle = isLight ? 'rgba(45,74,110,0.6)'  : 'rgba(200,223,245,0.6)';
    const tooltipBody  = isLight ? '#1a2f4a'              : '#e8f4ff';

    [healthChart, tempPressureChart, electricalChart, fuelChart].forEach(ch => {
        ch.options.scales.x.ticks.color  = tickColor;
        ch.options.scales.y.ticks.color  = tickColor;
        ch.options.scales.x.grid.color   = gridColor;
        ch.options.scales.y.grid.color   = gridColor;
        ch.options.plugins.legend.labels.color = legendColor;
        ch.options.plugins.tooltip.backgroundColor = tooltipBg;
        ch.options.plugins.tooltip.titleColor      = tooltipTitle;
        ch.options.plugins.tooltip.bodyColor       = tooltipBody;
        ch.update('none');
    });
}

// ═══════════════════════════════════════════════════════
// MESSAGE PROCESSOR (called from RAF drain)
// ═══════════════════════════════════════════════════════
function processMessage(data) {
    updateUI(data);
    updatePanels(data);
    pushPoint(data.timestamp, data);

    const lat = toNumber(data.lat);
    const lon = toNumber(data.lon);
    if (lat !== null && lon !== null) {
        const pt = [lat, lon];
        routePath.push(pt);
        if (routePath.length > MAX_STORE_POINTS) routePath.shift();
        trainMarker.setLatLng(pt);
        trainPath.setLatLngs(routePath);
        map.panTo(pt);
    }

    syncChartsFromStore();
    updateAllCharts();
}

// ═══════════════════════════════════════════════════════
// HISTORY PRELOAD
// ═══════════════════════════════════════════════════════
async function loadHistory() {
    try {
        const res = await fetch(`${HISTORY_URL}?limit=${MAX_STORE_POINTS}`, {
            headers: getAuthHeader()
        });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const history = await res.json();

        history.reverse().forEach(item => {
            pushPoint(item.timestamp, item);
            const lat = toNumber(item.lat);
            const lon = toNumber(item.lon);
            if (lat !== null && lon !== null) routePath.push([lat, lon]);
        });

        if (routePath.length > 0) {
            trainPath.setLatLngs(routePath);
            trainMarker.setLatLng(routePath[routePath.length - 1]);
            map.setView(routePath[routePath.length - 1], 13);
        }

        syncChartsFromStore();
        updateAllCharts();
        console.log(`[History] Loaded ${history.length} records`);

    } catch (e) {
        console.warn("[History] Unavailable:", e.message);
    }
}

// ═══════════════════════════════════════════════════════
// CSV EXPORT
// ═══════════════════════════════════════════════════════
async function downloadCSV() {
    try {
        const res  = await fetch(`${HISTORY_URL}?limit=${MAX_STORE_POINTS}`, {
            headers: getAuthHeader()
        });
        const data = await res.json();

        const header = "timestamp,speed,fuel_level,fuel_consumption,engine_temp,oil_temp,brake_pressure,voltage,current,health_index,lat,lon";
        const lines  = [header, ...data.map(d =>
            `${d.timestamp},${d.speed??''},${d.fuel_level??''},${d.fuel_consumption??''},` +
            `${d.engine_temp??''},${d.oil_temp??''},${d.brake_pressure??''},` +
            `${d.voltage??''},${d.current??''},${d.health_index??''},${d.lat??''},${d.lon??''}`
        )];

        const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8;" });
        const url  = URL.createObjectURL(blob);
        const a    = Object.assign(document.createElement("a"), { href: url, download: "locomotive_telemetry.csv" });
        a.click();
        URL.revokeObjectURL(url);

    } catch (e) {
        console.error("[CSV Export] Failed:", e);
        alert("Не удалось экспортировать данные.");
    }
}

// ═══════════════════════════════════════════════════════
// INIT — called after successful login
// ═══════════════════════════════════════════════════════
function initDashboard() {
    initMap();
    loadHistory().then(() => connectWS());
}