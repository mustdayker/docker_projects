
# Заходим

- **Airflow:** http://localhost:8080 Логин: `admin` Пароль: `admin`
- **Jupyter:** http://localhost:8888 Токен: `dataengineer`
- **Superset:** http://localhost:8088 Логин: `admin` Пароль: `admin`
- **Grafana:** http://localhost:3000 (`admin` / `admin`)
- **Prometheus:** http://localhost:9090
- **MinIO:** http://localhost:9001 (`minioadmin` / `minioadmin`)
- **SparkUI** http://localhost:8085

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

Проверьте Targets в Prometheus (http://localhost:9090/targets):
- ✅ prometheus (UP)
- ✅ postgres-exporter (UP)
- ✅ statsd-exporter (UP)
- ✅ node-exporter (UP)
- ✅ cadvisor (UP)


📊 Сервисы, с которых Prometheus собирает метрики:
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

🎯 Что вы можете анализировать:
- 📈 Производительность БД - через postgres-exporter
- 🚀 Работу Airflow - через statsd-exporter
- 🖥️ Ресурсы системы - через node-exporter
- 🐳 Состояние контейнеров - через cAdvisor


# Spark и MinIO

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LearnSpark") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Проверка
df = spark.range(1000)
df.show(5)
```

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MinIO Test") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

df = spark.range(10)
df.write.mode("overwrite").csv("s3a://learn-bucket/test-csv")
print("✅ Успешно!")
```

```python
spark = SparkSession.builder \
    .appName("Full Power") \
    .master("spark://spark-master:7077") \
    # ... твои S3A-настройки ...
    .config("spark.executor.instances", "2")      # по 1 executor’у на воркер
    .config("spark.executor.cores", "10")         # все ядра на executor
    .config("spark.executor.memory", "16g")       # оставляем 2 ГБ на overhead
    .config("spark.driver.memory", "4g")          # драйверу тоже нужно
    .getOrCreate()
```

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MinIO Test") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.cores", "10") \
    .config("spark.executor.memory", "16g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

df = spark.range(10)
df.write.mode("overwrite").csv("s3a://learn-bucket/test-csv")
print("✅ Успешно!")
```


### Создание бакета в MinIO:
Открой веб-консоль: http://localhost:9001
Логин: `minioadmin` / `minioadmin`
Создай бакеты: `learn-bucket`, `spark-logs`

### Шаг 6: Мониторинг в Grafana
Открой Grafana: http://localhost:3000 (admin/admin)
Добавь дашборды:
Для MinIO: импортируй официальный дашборд ID 13505 (https://grafana.com/grafana/dashboards/13505?spm=a2ty_o01.29997173.0.0.4346c921Wv4MBW)
Для Spark: можно использовать ID 12223 (https://grafana.com/grafana/dashboards/12223?spm=a2ty_o01.29997173.0.0.4346c921Wv4MBW) или создать свой
Убедись, что Prometheus видит таргеты: http://localhost:9090/targets

