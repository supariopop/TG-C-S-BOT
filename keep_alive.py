# keep_alive.py
import os
from threading import Thread
from flask import Flask

app = Flask('')

@app.route('/')
def home():
    return "✅ Blast Bot is alive & running!", 200

@app.route('/health')
def health():
    return {"status": "alive", "service": "sms-blast-bot"}, 200

@app.route('/ping')
def ping():
    return "pong", 200

def _run_server():
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

def keep_alive():
    """Start Flask keep-alive server in background daemon thread."""
    t = Thread(target=_run_server, daemon=True)
    t.start()
    print(f"🌐 [KEEP-ALIVE] Web server started on port {os.environ.get('PORT', 8080)}")