FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MASTER_HOST=0.0.0.0 \
    MASTER_PORT=9000 \
    MASTER_PANEL_HOST=0.0.0.0 \
    MASTER_PANEL_PORT=8080 \
    MASTER_DATA_DIR=/data

WORKDIR /app

RUN groupadd --system master \
    && useradd --system --gid master --home-dir /app --no-create-home master \
    && mkdir -p /data \
    && chown master:master /data

COPY --chown=master:master master.py master_center.html ./
COPY --chown=master:master macros/ ./macros/

USER master

EXPOSE 9000 8080
VOLUME ["/data"]

CMD ["python", "master.py"]
