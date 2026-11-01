FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MASTER_HOST=0.0.0.0 \
    MASTER_PORT=9000 \
    MASTER_PANEL_HOST=0.0.0.0 \
    MASTER_PANEL_PORT=8080 \
    MASTER_DATA_DIR=/data \
    HOME=/home/master

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends openssh-client sshpass \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system master \
    && useradd --system --gid master --home-dir /home/master --create-home master \
    && install -d -m 0700 -o master -g master /home/master/.ssh \
    && mkdir -p /data \
    && chown master:master /data

COPY --chown=master:master master.py master_center.html ./
COPY --chown=master:master master/ ./master/
COPY --chown=master:master macros/ ./macros/

USER master

EXPOSE 9000 8080
VOLUME ["/data"]

CMD ["python", "master.py"]
