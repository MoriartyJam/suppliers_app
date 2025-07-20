# Установка зависимостей
FROM python:3.11-slim

# Установка системных библиотек Playwright
RUN apt-get update && apt-get install -y \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libxkbcommon0 \
    libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libgbm1 libasound2 \
    libatspi2.0-0 libx11-xcb1 libdbus-1-3 wget curl && \
    apt-get clean

# Создание рабочего каталога
WORKDIR /app

# Копирование файлов
COPY . .

# Установка зависимостей Python
RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    playwright install --with-deps

# Использовать PORT от Render
ENV PORT=10000

# Команда запуска
CMD ["python", "app.py"]
