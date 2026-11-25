# Standalone 24/7 Docker image for EduShareVN Backend B (yt-dlp + FFmpeg + curl_cffi + Node JS runtime)
# Build from repository root:
#   docker build -f infra/all-mxh-deploy/Dockerfile -t edushare-ytdlp-worker .
FROM node:22-bookworm-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MALLOC_ARENA_MAX=2 \
    DOWNLOAD_DATA_DIR=/data \
    DOWNLOAD_WORKER_HOST=0.0.0.0 \
    DOWNLOAD_WORKER_PORT=8787 \
    DOWNLOAD_WORKER_ALL_SITES=1 \
    DOWNLOAD_MAX_WORKERS=1 \
    DOWNLOAD_DELETE_AFTER_STREAM=1 \
    DOWNLOAD_COMPLETED_TTL_SECONDS=600

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-venv ffmpeg ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m venv /opt/worker-venv

ENV PATH="/opt/worker-venv/bin:${PATH}"

COPY infra/libreget/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt

WORKDIR /app
COPY infra/libreget/worker.py /app/worker.py

RUN mkdir -p /data/files /data/tmp && chown -R node:node /app /data /opt/worker-venv
USER node

EXPOSE 8787

HEALTHCHECK --interval=30s --timeout=4s --start-period=10s --retries=3 \
  CMD python3 -c "import os, urllib.request; port=os.getenv('PORT') or os.getenv('DOWNLOAD_WORKER_PORT','8787'); urllib.request.urlopen(f'http://127.0.0.1:{port}/health', timeout=3)"

CMD ["sh", "-c", "DOWNLOAD_WORKER_PORT=${PORT:-${DOWNLOAD_WORKER_PORT:-8787}} exec python3 -u /app/worker.py"]
