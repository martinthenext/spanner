import os
from flask import Flask
from spanner import Spanner

app = Flask(__name__)


INPUT_BUCKET_NAME = "voices-bot-audio-files"


@app.route("/ingest")
def ingest():
    spanner = Spanner()
    spanner.ingest({"gcp": {"bucket": INPUT_BUCKET_NAME}})
    return {"status": "ok"}


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
