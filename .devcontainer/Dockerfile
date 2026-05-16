FROM python:3.11-slim
WORKDIR /usr/src/app
RUN apt-get update && apt-get install -y curl netcat-openbsd && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml README.md LICENSE end2end.py ./
COPY jobbers/ ./jobbers/
RUN pip install --upgrade pip && pip install -e ".[test]"
