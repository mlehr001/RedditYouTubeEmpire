"""
tts.py — Text-to-speech audio generation
Supports: gTTS (free), ElevenLabs (best quality), OpenAI TTS
Switch engine via TTS_ENGINE in .env
"""

import os
import config


def generate_audio(script, post_id):
    """
    Converts script text to an MP3 file.
    Returns the path to the generated audio file.
    """
    engine = config.TTS_ENGINE.lower()
    output_path = os.path.join(config.OUTPUT_DIR, f"{post_id}_audio.mp3")

    if engine == "gtts":
        return _gtts(script, output_path)
    elif engine == "elevenlabs":
        return _elevenlabs(script, output_path)
    elif engine == "openai":
        return _openai_tts(script, output_path)
    else:
        raise ValueError(f"Unknown TTS engine: {engine}. Use 'gtts', 'elevenlabs', or 'openai'.")


def _gtts(script, output_path):
    from gtts import gTTS
    tts = gTTS(text=script, lang=config.TTS_LANGUAGE, slow=config.TTS_SPEED)
    tts.save(output_path)
    return output_path


def _elevenlabs(script, output_path):
    from elevenlabs.client import ElevenLabs
    from elevenlabs import VoiceSettings

    client = ElevenLabs(api_key=os.getenv("ELEVENLABS_API_KEY"), timeout=120)

    # SDK 1.x: text_to_speech.convert() returns an iterator of audio chunks
    audio_stream = client.text_to_speech.convert(
        voice_id=config.ELEVENLABS_VOICE_ID,
        text=script,
        model_id="eleven_monolingual_v1",
        voice_settings=VoiceSettings(
            stability=config.ELEVENLABS_STABILITY,
            similarity_boost=config.ELEVENLABS_SIMILARITY_BOOST,
            style=config.ELEVENLABS_STYLE,
            use_speaker_boost=True,
        ),
    )

    with open(output_path, "wb") as f:
        for chunk in audio_stream:
            f.write(chunk)

    return output_path


def _openai_tts(script, output_path):
    from openai import OpenAI

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    response = client.audio.speech.create(
        model="tts-1",
        voice=config.OPENAI_TTS_VOICE,
        input=script,
    )
    response.stream_to_file(output_path)
    return output_path
