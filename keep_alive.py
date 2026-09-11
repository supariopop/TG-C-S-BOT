from flask import Flask
from threading import Thread
import os
import time

app = Flask(__name__)
_start_time = time.time()


@app.route("/")
@app.route("/health")
def health():
    return {
        "status": "ok",
        "uptime": int(time.time() - _start_time),
        "time": time.strftime("%Y-%m-%d %H:%M:%S"),
    }


@app.route("/ping")
def ping():
    return "pong"


def _run():
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port)


def keep_alive():
    """Start Flask in a background thread"""
    t = Thread(target=_run, daemon=True)
    t.start()
    print("🌐 Flask keep-alive server started")