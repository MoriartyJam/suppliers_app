# Используем официальный Python-образ
FROM python:3.11-slim

# Устанавливаем системные зависимости, нужные для WebKit
RUN apt-get update && apt-get install -y \
    wget gnupg curl ca-certificates \
    libnspr4 libnss3 libatk1.0-0 libatk-bridge2.0-0 \
    libcups2 libxkbcommon0 libatspi2.0-0 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libgbm1 libasound2 \
    libpangocairo-1.0-0 libgtk-3-0 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Устанавливаем Playwright и зависимости Python
WORKDIR /app
COPY . /app

RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    playwright install

# Указываем порт для Seenode
ENV PORT=80

# Запуск приложения
CMD ["python", "app.py"]
