FROM python:3.11-slim

# Переменные окружения
ENV PLAYWRIGHT_BROWSERS_PATH=/app/.cache/ms-playwright
ENV DEBIAN_FRONTEND=noninteractive
ENV PORT=10000

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    wget curl gnupg2 ca-certificates \
    libglib2.0-0 libnss3 libatk1.0-0 \
    libatk-bridge2.0-0 libcups2 libxkbcommon0 \
    libxcomposite1 libxdamage1 libxrandr2 libgbm1 libasound2 \
    libatspi2.0-0 libx11-xcb1 libdbus-1-3 \
    libgtk-3-0 libgdk-pixbuf-2.0-0 libcairo2 \
    libpango-1.0-0 libpangocairo-1.0-0 libharfbuzz0b \
    libicu72 libxml2 libxslt1.1 liblcms2-2 libwebp7 libjpeg62-turbo \
    libpng16-16 libfreetype6 libfontconfig1 libenchant-2-2 libsecret-1-0 \
    libwayland-egl1 libwayland-client0 libgles2 libx264-dev \
    xvfb libepoxy0 --no-install-recommends && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Установка рабочей директории
WORKDIR /app

# Копирование файлов проекта
COPY . .

# Установка Python-зависимостей и Playwright
RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    playwright install --with-deps

# Запуск приложения
CMD ["python", "app.py"]
