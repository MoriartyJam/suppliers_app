FROM python:3.11-slim

# Установка зависимостей ОС
RUN apt-get update && apt-get install -y wget gnupg ca-certificates curl \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libxkbcommon0 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libgbm1 libasound2 libatspi2.0-0 \
    libx11-xcb1 libgtk-3-0 libxshmfence1 libxss1 libxext6 libx11-6 libdrm2 \
    libglu1-mesa libwayland-client0 libwayland-egl1-mesa libwayland-server0 \
    fonts-liberation libappindicator3-1 lsb-release xdg-utils

# Установка зависимостей Python
WORKDIR /app
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Установка Playwright и браузеров
RUN playwright install --with-deps

# Копирование приложения
COPY . .

# Запуск
CMD ["flask", "--app", "app.py", "run", "--host", "0.0.0.0", "--port", "80"]
