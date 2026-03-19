FROM python:3.11-slim

# System dependencies: FFmpeg for video, ImageMagick for text overlays
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    imagemagick \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Output directories
RUN mkdir -p /data/videos /app/assets/fonts /app/logs

CMD ["python", "-m", "story_engine.scheduler"]
