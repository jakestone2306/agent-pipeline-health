import os
import threading
import traceback
from datetime import datetime
from flask import Flask, jsonify
from pipeline_agent import run

app = Flask(__name__)

# Track run state
run_state = {"status": "idle", "last_run": None, "last_result": None, "last_error": None}

def run_in_background():
    run_state["status"] = "running"
    run_state["last_run"] = datetime.utcnow().isoformat()
    try:
        result = run()
        run_state["status"] = "success"
        run_state["last_result"] = result
        run_state["last_error"] = None
    except Exception as e:
        run_state["status"] = "error"
        run_state["last_error"] = traceback.format_exc()
        print(f"Agent error: {e}")

@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "agent": "pipeline-health-agent", "run_state": run_state})

@app.route("/run", methods=["POST"])
def trigger_run():
    if run_state["status"] == "running":
        return jsonify({"status": "already_running", "message": "Agent is already running"}), 409
    thread = threading.Thread(target=run_in_background)
    thread.daemon = True
    thread.start()
    return jsonify({"status": "started", "message": "Pipeline health agent started in background. Check /status for updates."})

@app.route("/status", methods=["GET"])
def status():
    return jsonify(run_state)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
