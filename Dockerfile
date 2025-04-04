FROM python:3.11
WORKDIR /usr/src/app
COPY ./jobbers ./jobbers
COPY otel-config.yaml .
COPY pyproject.toml .
COPY LICENSE .
COPY README.md .
RUN pip3 install --no-cache-dir -e .
EXPOSE 8000
