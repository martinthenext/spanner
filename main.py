import os
from flask import Flask
from spanner import spanner as sp

app = Flask(__name__)


INPUT_BUCKET = "voices-bot-audio-files"
APP_NAME = "voices-bot"


def voices_bot_get_user_id(filename):
    return filename.split("-")[-3]


def voices_bot_get_group_id(filename):
    return filename.split("-")[-2]


@app.route("/ingest")
def ingest():
    #voices = sp.ingest(
    #    "voice",
    #    {"gcp": {"bucket": INPUT_BUCKET}, "suffix": ".oga"},
    #    app=APP_NAME,
    #    context=voices_bot_get_group_id,
    #    author=voices_bot_get_user_id,
    #)
    
    return {"status": "ok"}


if __name__ == "__main__": 
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
