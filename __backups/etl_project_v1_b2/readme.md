
# Заходим

- **Airflow:** http://localhost:8080 Логин: `admin` Пароль: `admin`
- **Jupyter:** http://localhost:8888 Токен: `dataengineer`
- **Superset:** http://localhost:8088 Логин: `admin` Пароль: `admin`

### PostgreSQL - База данных
Подключение:
- Хост: `localhost`
- Порт: `5432`
- База: `airflow` (для Airflow) или `learn_base` (для ваших данных)
- Пользователь: `airflow`
- Пароль: `airflow`

### Папки

- **Ноутбуки:** `./jupyter/notebooks/`
- **DAG'и Airflow:** `./airflow/dags/`
- **Плагины Airflow:** `./airflow/plugins/`
- **Логи Airflow:** `./airflow/logs/`
- **Данные Superset:** `./superset/data/`


### Вход в контейнеры (для отладки):
```bash
# Войти в контейнер с bash
docker exec -it jupyter-lab bash
docker exec -it airflow-scheduler bash
docker exec -it superset bash
```

### Просмотр логов:
```bash
# Все сервисы
docker compose logs

# Конкретный сервис
docker compose logs airflow-webserver
docker compose logs jupyter
docker compose logs superset
```



### Чистим Volume

```bash
# Удаляет все неиспользуемые тома (включая анонимные)
docker volume prune

# Но будьте осторожны - удалит ВСЕ неиспользуемые тома
# Лучше сначала посмотреть что будет удалено:
docker volume ls -f "dangling=true"
```

# Команды для запуска:

### При первом запуске надо проинициализировать `Superset`

```bash
# 2. Создаем папки для данных
mkdir -p airflow/dags airflow/logs airflow/plugins jupyter/notebooks superset/data

# 3. Запускаем все сервисы
docker compose up -d
docker compose down

# 5. Инициализируем Superset
docker compose exec superset superset db upgrade
docker compose exec superset superset fab create-admin --username admin --firstname Admin --lastname User --email admin@example.com --password admin
docker compose exec superset superset init

# Доп команды

# Удалить Noname контейнеры
docker volume prune
```

## Grafana / Prometheus

#### Что было изменено в существующих сервисах:
1. Airflow Scheduler & Webserver: Добавлены переменные окружения для включения экспорта метрик через StatsD
2. Добавлены зависимости: Airflow сервисы теперь зависят от statsd-exporter

#### Что вы получите:
- Prometheus (порт 9090) - сбор и хранение метрик
- Grafana (порт 3000) - дашборды и визуализация
- Postgres Exporter - метрики PostgreSQL
- StatsD Exporter - преобразование метрик Airflow в Prometheus формат

Теперь вы можете:
- Настроить дашборды в Grafana (логин: admin/admin)
- Изучать метрики в Prometheus
- Экспериментировать с разными типами визуализаций
- Мониторить производительность всех компонентов вашего стека

## Доступ к интерфейсам
### Prometheus
- URL: http://localhost:9090
- Назначение: Просмотр метрик, создание запросов (PromQL), проверка targets

### Grafana
- URL: http://localhost:3000
- Логин: admin
- Пароль: admin
- Назначение: Визуализация метрик, дашборды