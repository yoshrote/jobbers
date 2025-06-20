FROM python:3.11-slim
WORKDIR /usr/src/app
COPY otel-config.yaml .
COPY pyproject.toml .
COPY LICENSE .
COPY README.md .
COPY ./jobbers/__init__.py ./jobbers/__init__.py
RUN pip3 install --upgrade pip && pip3 install --no-cache-dir -e .
RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends gcc && apt-get clean && rm -rf /var/lib/apt/lists/*
COPY ./jobbers ./jobbers
EXPOSE 8000
