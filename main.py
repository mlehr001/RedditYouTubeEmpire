"""
RedditYouTubeEmpire — Entry point

CH1 (CHANNEL_TYPE=story):   Reddit personal stories → script → TTS → b-roll → edit → upload
CH2 (CHANNEL_TYPE=mystery): Mystery Top 5 countdown → script → TTS → number cards → edit → upload
"""

import os
from dotenv import load_dotenv

load_dotenv()

import config


def run():
    channel = config.CHANNEL_TYPE

    if channel == "mystery":
        print("\n[CH2 MYSTERY] Mystery Top 5 pipeline starting...\n")
        from pipelines.mystery import run_mystery
        run_mystery()
    else:
        print("\n[CH1 STORY] Reddit story pipeline starting...\n")
        from pipelines.story import run_story
        run_story()


if __name__ == "__main__":
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    os.makedirs(config.ASSETS_DIR, exist_ok=True)
    run()
