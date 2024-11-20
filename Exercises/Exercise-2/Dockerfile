FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY . /app

RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install -r requirements.txt
