
## Ссылки на сервисы

| Сервис         | Ссылка               | Учетные данные              |
|----------------|----------------------|-----------------------------|
| **Jupyter**    | http://localhost:8888 | Токен: `dataengineer`       |
| **Airflow**    | http://localhost:8080 | Логин: `admin` Пароль: `admin` |
| **Superset**   | http://localhost:8088 | Логин: `admin` Пароль: `admin` |
| **MinIO**      | http://localhost:9001 | Логин: `minioadmin` Пароль: `minioadmin` |
| **Grafana**    | http://localhost:3000 | Логин: `admin` Пароль: `admin` |
| **Prometheus** | http://localhost:9090 | -                           |

## Мониторинг

| Сервис                     | Ссылка |
|----------------------------|--------|
| **SparkUI - master**       | http://localhost:8085 |
| **SparkUI - worker 1**     | http://localhost:8086/ |
| **SparkUI - worker 2**     | http://localhost:8087/ |
| **KafkaUI**                | http://localhost:8082/ |
| **Spark Master метрики**   | http://localhost:8085/metrics/prometheus/ |
| **Spark Worker 1 метрики** | http://localhost:8086/metrics/prometheus/ |
| **Spark Worker 2 метрики** | http://localhost:8087/metrics/prometheus/ |
| **cAdvisor**               | http://localhost:8081/ |

## PostgreSQL

| Параметр         | Значение |
|------------------|----------|
| **Хост**         | `localhost` / `postgres-db` |
| **Порт**         | `5432` |
| **Базы**         | `airflow` (для Airflow) и `learn_base` (для данных) |
| **Пользователь** | `airflow` |
| **Пароль**       | `airflow` |


## Kafka

Чтобы запустить Producer надо активировать контейнер `kafka-producer`. По умолчанию он не стартует

```bash 
# для запуска
docker-compose --profile manual up -d kafka-producer

# Смотрим логи генератора
docker-compose logs -f kafka-producer

# Останавливаем генератор
docker-compose --profile manual stop kafka-producer

# Или полностью удаляем контейнер
docker-compose --profile manual down kafka-producer

```

## Папки

Папка | Описание
-|-
`./airflow/dags` | Даги
`./airflow/module` | Кастомные модули
`./airflow/tasks` | Таски упакованные в `.py` скрипты
`./images` | Скаченные образы для ускорения сборки `compose`
`./jupyter/work/` | Ноутбуки
`./jupyter/work/datasets` | Папка смонтирована в `shared_data` в корне контейнеров `spark` и `airflow`
`./jupyter/work/module` | Кастомные модули `.py`
`./jupyter/work/spark/apps` | Смонтированный сквозной `volume` с контейнером `spark` 
`./requirements` | Зависимости для сборки контейнеров `Airflow` и `Jupyter`
`./spark/apps` | Приложения `Spark`, папка прокинута между `Airflow` и `Jupyter`  
`./spark/conf` | Конфигурация `Spark` 
`./spark/external-jars` | Предзагруженные зависимости 
`./superset/data/` | БД `Superset`
`.monitoring` | Параметры для метрик и дашборды `Grafana`



# ЗАПУСК ПРОЕКТА

## Важно!!!
- `Dockerfile` и `compose` используют локальные архивы с образами
- Чтобы проект собрался надо создать папку `images` в корне проекта (она не синхронизируется с `github`)
- Скачать туда архив: https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz 
- Он душный и медленно качается, если это делать через интерфейс командной строки, непонятно завис процесс или нет
- Остальное скачается из `Dockerhub` для ускорения загрузки читай комменты в `Dockerfile`

```bash
# Запускаем все сервисы
docker compose up -d
docker compose down
```

### При первом запуске надо проинициализировать `Superset`

```bash
# Инициализируем Superset
# По очереди выполняем каждую команду
docker compose exec superset superset db upgrade
docker compose exec superset superset fab create-admin --username admin --firstname Admin --lastname User --email admin@example.com --password admin
docker compose exec superset superset init
```

### Зависимости
- Все зависимости лежат в папке `requirements`
- Если были добавлены новые надо пересобрать образы `jupyter` и `airflow`
- Для этого выполняем 

```bash
docker compose down
docker compose build jupyter
docker compose build airflow-init airflow-scheduler airflow-webserver
docker compose up -d
````

# Команды

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

# Аккуратно! удалит ВСЕ неиспользуемые тома
# Лучше сначала посмотреть что будет удалено:
docker volume ls -f "dangling=true"
```

# 📊 Сервисы, с которых Prometheus собирает метрики:
#### 1. PostgreSQL + Экспортер ✅
- Сервис: postgres-exporter:9187
- Что мониторит:
- Производительность БД
- Подключения, транзакции, запросы
- Размеры баз данных
- Кэш и блокировки

#### 2. Airflow + StatsD ✅
- Сервис: statsd-exporter:9102
- Что мониторит:
- DAG runs и задачи
- Планировщик (scheduler)
- Исполнители (executors)
- Очереди и пулы

#### 3. Системные метрики ✅
- Сервис: node-exporter:9100
- Что мониторит:
- CPU, память, диск хоста
- Сеть, нагрузка, процессы

#### 4. Метрики контейнеров ✅
- Сервис: cadvisor:8080
- Что мониторит:
- Ресурсы каждого контейнера (CPU, память)
- Сеть и I/O контейнеров
- Использование диска

#### 5. Сам Prometheus ✅
- Сервис: localhost:9090
- Что мониторит:
- Собственное состояние
- Количество собираемых метрик
- Производительность сбора


## Локальное использование образов

После первой сборки проекта можно сохранить образы локально
и далее переносить их на другой ПК, чтобы не качать каждый раз заново.

#### Сохранить скачанный образ из Docker

```bash
docker save -o images/apache-airflow-2.7.1.tar apache/airflow:2.7.1
docker save -o images/etl_project_v1-airflow-init.tar etl_project_v1-airflow-init:latest 
docker save -o images/etl_project_v1-airflow-scheduler.tar etl_project_v1-airflow-scheduler:latest
docker save -o images/etl_project_v1-airflow-webserver.tar etl_project_v1-airflow-webserver:latest 
docker save -o images/etl_project_v1-jupyter.tar etl_project_v1-jupyter:latest
docker save -o images/postgres-15-alpine.tar postgres:15-alpine
docker save -o images/spark-3.5.0.tar apache/spark:3.5.0
docker save -o images/superset-latest.tar amancevice/superset:latest
docker save -o images/minio-latest.tar minio/minio:latest
docker save -o images/prometheus-latest.tar prom/prometheus:latest
docker save -o images/grafana-latest.tar grafana/grafana:latest
docker save -o images/node-exporter-latest.tar prom/node-exporter:latest
docker save -o images/statsd-exporter-latest.tar prom/statsd-exporter:latest
docker save -o images/cadvisor-latest.tar gcr.io/cadvisor/cadvisor:latest
docker save -o images/postgres-exporter-latest.tar prometheuscommunity/postgres-exporter:latest
```

#### Загрузить локальный образ в Docker

```bash
docker load -i apache-airflow-2.7.1.tar
docker load -i etl_project_v1-airflow-init.tar
docker load -i etl_project_v1-airflow-scheduler.tar
docker load -i etl_project_v1-airflow-webserver.tar
docker load -i etl_project_v1-jupyter.tar
docker load -i postgres-15-alpine.tar
docker load -i spark-3.5.0.tar
docker load -i superset-latest.tar
docker load -i minio-latest.tar
docker load -i prometheus-latest.tar
docker load -i grafana-latest.tar
docker load -i node-exporter-latest.tar
docker load -i statsd-exporter-latest.tar
docker load -i cadvisor-latest.tar
docker load -i postgres-exporter-latest.tar
```

После этого при сборке проекта Docker не будет тащить образ из интернета