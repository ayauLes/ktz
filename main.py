"""
main.py — Единая точка запуска КТЖ Цифрового двойника

Порядок запуска:
  1. generator.py   — генерация телеметрии → rawdata.db
  2. processor.py   — обработка телеметрии → processed_telemetry
  3. server.py      — FastAPI + WebSocket сервер (uvicorn, порт 8000)
  4. index.html     — открывается в браузере автоматически

Остановка: Ctrl+C — завершает все процессы корректно.
"""

import os
import sys
import time
import signal
import subprocess
import webbrowser
import threading

# ─────────────────────────────────────────────
# ПУТИ
# ─────────────────────────────────────────────
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR  = os.path.join(BASE_DIR, "backend")
FRONTEND_DIR = os.path.join(BASE_DIR, "frontend")
INDEX_HTML   = os.path.join(FRONTEND_DIR, "index.html")

PYTHON       = sys.executable   # тот же интерпретатор, которым запущен main.py

SERVER_HOST  = "127.0.0.1"
SERVER_PORT  = 8000
SERVER_URL   = f"http://{SERVER_HOST}:{SERVER_PORT}"
SERVER_READY_TIMEOUT = 15       # секунд ждём пока сервер поднимется

# ─────────────────────────────────────────────
# ЦВЕТА ДЛЯ ТЕРМИНАЛА
# ─────────────────────────────────────────────
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def log(color, tag, msg):
    print(f"{color}{BOLD}[{tag}]{RESET} {msg}")

# ─────────────────────────────────────────────
# ЗАПУСК СУБПРОЦЕССА
# ─────────────────────────────────────────────
processes = []

def start_process(name, script, cwd):
    """Запускает python-скрипт как субпроцесс, добавляет в список для cleanup."""
    path = os.path.join(cwd, script)
    if not os.path.exists(path):
        log(RED, "ERROR", f"Файл не найден: {path}")
        shutdown(exit_code=1)

    proc = subprocess.Popen(
        [PYTHON, path],
        cwd=cwd,
        # stdout/stderr остаются на терминал — видны все логи субпроцессов
    )
    processes.append((name, proc))
    log(GREEN, name, f"Запущен (PID {proc.pid})")
    return proc

def start_uvicorn(cwd):
    """Запускает uvicorn для server.py."""
    proc = subprocess.Popen(
        [
            PYTHON, "-m", "uvicorn",
            "server:app",
            "--host", SERVER_HOST,
            "--port", str(SERVER_PORT),
            "--reload",
        ],
        cwd=cwd,
    )
    processes.append(("SERVER", proc))
    log(GREEN, "SERVER", f"uvicorn запущен (PID {proc.pid})  →  {SERVER_URL}")
    return proc

# ─────────────────────────────────────────────
# ОЖИДАНИЕ ГОТОВНОСТИ СЕРВЕРА
# ─────────────────────────────────────────────
def wait_for_server(timeout=SERVER_READY_TIMEOUT):
    """Пингует GET / до тех пор пока сервер не ответит 200 или не истечёт таймаут."""
    import urllib.request
    import urllib.error

    log(YELLOW, "WAIT", f"Ожидаю готовности сервера (до {timeout}с)...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(SERVER_URL, timeout=1)
            return True
        except Exception:
            time.sleep(0.5)
    return False

# ─────────────────────────────────────────────
# ОТКРЫТИЕ БРАУЗЕРА
# ─────────────────────────────────────────────
def open_browser():
    url = f"file:///{INDEX_HTML.replace(os.sep, '/')}"
    log(CYAN, "BROWSER", f"Открываю {url}")
    webbrowser.open(url)

# ─────────────────────────────────────────────
# МОНИТОРИНГ ПРОЦЕССОВ
# ─────────────────────────────────────────────
def monitor_processes():
    """Следит за субпроцессами и завершает всё если один из них упал."""
    while True:
        time.sleep(2)
        for name, proc in processes:
            ret = proc.poll()
            if ret is not None:
                log(RED, "MONITOR", f"{name} завершился с кодом {ret} — останавливаю всё")
                shutdown(exit_code=1)

# ─────────────────────────────────────────────
# КОРРЕКТНОЕ ЗАВЕРШЕНИЕ
# ─────────────────────────────────────────────
def shutdown(exit_code=0):
    log(YELLOW, "SHUTDOWN", "Завершаю все процессы...")
    for name, proc in reversed(processes):
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=4)
                log(GREEN, "SHUTDOWN", f"{name} остановлен")
            except subprocess.TimeoutExpired:
                proc.kill()
                log(YELLOW, "SHUTDOWN", f"{name} принудительно завершён (kill)")
    log(GREEN, "SHUTDOWN", "Готово. До свидания!")
    sys.exit(exit_code)

def handle_signal(sig, frame):
    print()  # новая строка после ^C
    shutdown(exit_code=0)

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    signal.signal(signal.SIGINT,  handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    print()
    print(f"{CYAN}{BOLD}{'─'*50}{RESET}")
    print(f"{CYAN}{BOLD}  КТЖ · Цифровой двойник локомотива{RESET}")
    print(f"{CYAN}{BOLD}{'─'*50}{RESET}")
    print()

    # 1. Генератор телеметрии
    start_process("GENERATOR", "generator.py", BACKEND_DIR)
    time.sleep(1)   # даём генератору создать БД и записать первые строки

    # 2. Процессор телеметрии
    start_process("PROCESSOR", "processor.py", BACKEND_DIR)
    time.sleep(0.5)

    # 3. Сервер FastAPI
    start_uvicorn(BACKEND_DIR)

    # 4. Ждём пока сервер реально поднимется
    ready = wait_for_server()
    if not ready:
        log(RED, "ERROR", f"Сервер не ответил за {SERVER_READY_TIMEOUT}с — проверьте логи выше")
        shutdown(exit_code=1)

    log(GREEN, "READY", f"Сервер готов → {SERVER_URL}")
    print()
    log(CYAN, "DOCS",  f"Swagger UI → {SERVER_URL}/docs")
    print()

    # 5. Открываем браузер
    open_browser()

    # 6. Мониторинг в фоне
    t = threading.Thread(target=monitor_processes, daemon=True)
    t.start()

    log(YELLOW, "INFO", "Нажмите Ctrl+C для остановки всех процессов")
    print()

    # Держим main поток живым
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()