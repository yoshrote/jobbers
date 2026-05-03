FROM python:3.11.18-slim
WORKDIR /usr/src/app
RUN apt-get update && apt-get install -y curl netcat-openbsd
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
COPY otel-config.yaml .
COPY pyproject.toml .
COPY LICENSE .
COPY README.md .
COPY ./jobbers ./jobbers
RUN pip install --upgrade pip && pip install --no-cache-dir -e .
COPY ./end2end.py ./end2end.py
# RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends gcc && apt-get clean && rm -rf /var/lib/apt/lists/*
EXPOSE 8000
