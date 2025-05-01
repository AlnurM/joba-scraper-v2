# syntax=docker/dockerfile:1

FROM mcr.microsoft.com/playwright/python:latest


WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

RUN playwright install --with-deps


COPY . .


ENV PORT=${PORT}

# Запускаем приложение
CMD ["python", "main.py"]
