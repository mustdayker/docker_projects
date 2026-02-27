FROM python:3.10-slim

WORKDIR /app

# Копируем зависимости и устанавливаем их
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем скрипт генератора
COPY producer.py .

# Команда по умолчанию (будет переопределяться при запуске)
CMD ["python", "producer.py"]