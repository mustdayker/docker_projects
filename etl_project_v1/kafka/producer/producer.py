#!/usr/bin/env python
"""
Генератор случайных событий для Kafka.
Отправляет 5 сообщений в секунду в топик 'user-events'.
"""
import json
import random
import time
from datetime import datetime, date
from faker import Faker
from kafka import KafkaProducer

# Инициализация Faker для генерации случайных username
fake = Faker()

# Настройки подключения к Kafka
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'  # Имя сервиса из docker-compose
KAFKA_TOPIC = 'user-events'

# Создаем продюсера с сериализацией JSON
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
    # default=str нужен для сериализации date/datetime в строку
    acks='all'  # Ждем подтверждения от всех реплик для надежности
)


def generate_event():
    """Генерирует одно случайное событие"""
    return {
        'id': random.randint(1, 1000000),  # Случайный ID
        'date': date.today().isoformat(),  # Текущая дата
        'event_date': datetime.now().isoformat(),  # Текущее время с секундами
        'event_id': random.randint(1, 5),  # Случайное событие 1-5
        'username': fake.user_name(),  # Случайный username
        'group_id': random.randint(1, 5),  # Случайная группа 1-5
        'value': round(random.uniform(0, 10000), 2)  # Случайное число 0-10000 с 2 знаками
    }


def main():
    print(f"Генератор запущен. Отправка в топик: {KAFKA_TOPIC}")
    print("Нажмите Ctrl+C для остановки")

    try:
        while True:
            # Генерируем 5 сообщений
            for _ in range(5):
                event = generate_event()
                # Отправляем сообщение в Kafka
                future = producer.send(KAFKA_TOPIC, value=event)
                # Блокируемся до получения подтверждения (для наглядности)
                record_metadata = future.get(timeout=10)
                print(f"Отправлено: {event['id']} | "
                      f"Топик: {record_metadata.topic} | "
                      f"Партиция: {record_metadata.partition} | "
                      f"Offset: {record_metadata.offset}")

            # Ждем 1 секунду перед следующей пачкой (5 сообщений/секунду)
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nОстановка генератора...")
    finally:
        producer.close()
        print("Генератор остановлен")


if __name__ == "__main__":
    main()