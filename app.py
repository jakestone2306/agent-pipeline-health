import os
from flask import Flask, jsonify
from pipeline_agent import run

app = Flask(__name__)

@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "agent": "pipeline-health-agent"})

@app.route("/run", methods=["POST"])
def run_agent():
    try:
        result = run()
        return jsonify({"status": "success", "result": result})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
