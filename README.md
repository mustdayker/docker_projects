## Дата-стенд `etl_project_v1`

> - **Внимание!** Весь стенд уверенно кушает **30GB RAM** (без Spark сессий)
> - **Spark Worker** (2 шт.) при запуске кушают по 8GB и 5 ядер на каждый, (1 сессия 16GB и 10 ядер) омном

### 🗄️ Хранилище данных:
- `PostgreSQL` - основная БД для Airflow и Superset
- `MinIO` - объектное хранилище (аналог S3)
- `Clickhouse` - колоночная аналитическая БД
- `MongoDB` - документо-ориентированная БД
- `Redis` - in-memory кэш

### ⚙️ Управление данными:
- `Airflow` - оркестрация ETL/ELT процессов
- `Spark` - распределенная обработка данных (мастер + 2 воркера)
- `Kafka` - брокер сообщений
- `Iceberg` - DataLake > Lakehouse
- `Trino` - Распределенный движок SQL запросов

### 📊 Аналитика и визуализация:
- `Jupyter` - интерактивная разработка ноутбуков
- `Superset` - BI-платформа для дашбордов и аналитики

### 📈 Мониторинг:
- `Prometheus` - сбор и хранение метрик
- `Grafana` - визуализация метрик и дашборды
- `Node Exporter` - метрики системы
- `cAdvisor` - мониторинг контейнеров
- `StatsD Exporter` - преобразование метрик Airflow для Prometheus

## To Do List
- `Iceberg`
- `Trino`
- `dbt`
- `Векторные БД (Qdrant)`
- `FastAPI`
- `Data Quality`
- `Spark History Server`
- `Spark ML`
- `Kubernetes`
- `GPU in data`
- `Data Architect`, `Platform Engineer`

## Где брать датасеты

- [`https://vc.ru/links/1977139-gde-najti-besplatnye-datasety-dlya-proyektov`](https://vc.ru/links/1977139-gde-najti-besplatnye-datasety-dlya-proyektov)
- [`https://www.kaggle.com/datasets`](https://www.kaggle.com/datasets)
- [`https://catalog.data.gov/dataset`](https://catalog.data.gov/dataset/)
- [`https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page`](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)



### Spark stack
- Партиционирование по ключам JOIN
- `broadcat join` маленьких таблиц 
  > - `from pyspark.sql.functions import broadcast`
- Кэш таблиц 
  > - `from pyspark import StorageLevel`
  > - `.cache() / .persist() / .unpersist()`)
  



