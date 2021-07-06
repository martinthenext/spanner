import os
from flask import Flask
from spanner import spanner as sp

app = Flask(__name__)


INPUT_BUCKET = "voices-bot-audio-files"
APP_NAME = "voices-bot"


@app.route("/ingest")
def ingest():
    #TODO Read data from GCP and run spanner on it
    
    return {"status": "ok"}


if __name__ == "__main__": 
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
