### USER
 У меня есть тестовый стенд собранный на Docker. 

Там есть такой набор сервисов:

- `PostgreSQL`
- `Airflow`
- `Superset`
- `Prometheus` 
- `Grafana`
- `Jupyterlab`
- `MinIO`
- `Spark`

Я учусь профессии Data Engineer. 

Придумай для меня задание на этом стенде чтобы мне потренировать свои навыки. 
Возьми какой нибудь большой датасет из открытых источников, чтобы я ощутил всю прелесть Spark и параллельных вычислени

### ASSISTANT
Отлично! Учитывая твой стек (Spark, MinIO как S3-совместимое хранилище, PostgreSQL, Airflow, JupyterLab и т.д.), я предложу тебе комплексное задание, которое задействует почти все компоненты стенда и даст тебе почувствовать силу распределённых вычислений Spark на большом объёме данных.

---

### 🎯 **Задание: ETL-пайплайн анализа авиаперелётов в США**

#### 📦 **Исходные данные**
Используй открытый датасет **"US Domestic Airline Flights Performance"** от [Bureau of Transportation Statistics (BTS)](https://www.transtats.bts.gov/).  
Особенно подходит датасет **"On-Time Performance"** — он содержит миллионы записей о рейсах с 1987 по 2024 год.

🔗 Прямая ссылка на архивы (например, за 2023 год):  
https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGK&QO_fu146_anil=3&gnoyr_VQ=None&airline=All&Airport=All&dtId=975&geo=All&crp=All&geo=All&crp=All&flg=All&flg=All&Submit=Submit

https://transtats.bts.gov/PREZIP/

- `On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2023_1`

https://catalog.data.gov/dataset/on-time-performance

Для старта возьми **данные за 2020–2023 годы** (по ~7–10 млн строк в год). Это даст тебе **~30–40 млн строк**, что отлично подходит для Spark.

Формат: CSV (можно конвертировать в ORC/Parquet позже).

---

### 🧩 **Этапы задания**

#### 1. **Загрузка данных в MinIO**
- Скачай несколько файлов (например, по годам) локально.
- Загрузи их в бакет MinIO (например, `raw-flights-csv`) через `mc` (MinIO Client) или Python-скрипт.
- Убедись, что Spark может читать из MinIO через S3A-коннектор.

#### 2. **Spark-обработка: ETL**
В JupyterLab (локально или в контейнере) напиши PySpark-скрипт, который:
- Читает все CSV из MinIO (`s3a://raw-flights-csv/`).
- Выполняет очистку:
  - Удаляет строки с пустыми значениями в ключевых полях (`ORIGIN`, `DEST`, `DEP_DELAY`, `ARR_DELAY` и т.д.).
  - Преобразует типы (даты, числа).
- Добавляет производные поля:
  - `IS_DELAYED` (если `ARR_DELAY > 15`)
  - `ROUTE` = `ORIGIN + '-' + DEST`
  - `HOUR_OF_DAY` из времени вылета
- Сохраняет результат в **ORC или Parquet** в новый бакет MinIO: `processed-flights-parquet`.

💡 **Цель**: почувствуй разницу между обработкой 1 млн и 30 млн строк. Попробуй настроить Spark на использование 2 воркеров с полной загрузкой CPU и памяти (как ты хотел).

#### 3. **Агрегация и аналитика**
Создай агрегированные таблицы:
- Топ-20 самых задерживаемых маршрутов.
- Средняя задержка по аэропортам вылета.
- Динамика задержек по месяцам/часам.

Сохрани их как таблицы в **PostgreSQL** (через JDBC из Spark или через pandas + SQLAlchemy из Jupyter).

#### 4. **Визуализация в Superset**
- Подключи Superset к PostgreSQL.
- Создай дашборд:
  - Карта США с аэропортами и задержками.
  - График задержек по времени суток.
  - Таблица топ-маршрутов.

#### 5. **Оркестрация через Airflow**
- Создай DAG в Airflow, который:
  1. Проверяет наличие новых файлов в MinIO (или просто запускает ETL).
  2. Запускает Spark-приложение (через `SparkSubmitOperator` или через `BashOperator`, если Spark запущен в standalone-режиме).
  3. После завершения — обновляет Superset (опционально: через REST API или просто логирует успех).

#### 6. **Мониторинг**
- Настрой сбор метрик Spark (через Prometheus JMX Exporter или Spark’s built-in metrics) → Prometheus → Grafana.
- Визуализируй:
  - Использование CPU/памяти воркеров.
  - Длительность стадий Spark.
  - Пропускную способность MinIO (если возможно).

---

### 🧠 Что ты прокачаешь:
- Работу с **большими данными** в Spark (партиционирование, сериализация, оптимизация).
- Интеграцию **Spark + MinIO (S3A)**.
- **ETL-процессы** от сырых данных до аналитических таблиц.
- **Оркестрацию** через Airflow.
- **Визуализацию** через Superset.
- **Мониторинг** инфраструктуры.
- Практику **data lakehouse** (MinIO как lake, PostgreSQL как warehouse).

---

Если хочешь — могу дать шаблон PySpark-кода для чтения из MinIO или пример DAG в Airflow. Готов продолжить!

### USER
Да. Подготовь для меня какие то заготовки, чтобы мне было проще начать. Я совсем новичок 

### ASSISTANT
Отлично! Раз ты новичок — я подготовлю для тебя **минимальные, но рабочие заготовки**, чтобы ты мог быстро запустить первый этап: **чтение CSV из MinIO в Spark через JupyterLab** и сохранить результат в Parquet/ORC обратно в MinIO.

Всё будет адаптировано под твой Docker-стенд.

---

## 📁 1. Подготовка MinIO

### Создай бакеты (через веб-интерфейс MinIO или `mc`):
- `raw-flights-csv` — для исходных CSV
- `processed-flights` — для обработанных данных (Parquet/ORC)

> Логин/пароль по умолчанию (если не менял): `minioadmin` / `minioadmin`

---

## 📥 2. Скачай и загрузи данные

### Пример: данные за январь 2023 года
Скачай один файл для старта (это ~600k строк — быстро обработается):

🔗 https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2023_1.zip

Распакуй → получишь CSV → залей в MinIO в бакет `raw-flights-csv` как `2023-01.csv`.

> 💡 Совет: переименуй колонки в CSV, если они содержат пробелы или спецсимволы — Spark не любит такие имена. Можно вручную заменить в первой строке:
> ```
> FL_DATE,OP_CARRIER,OP_CARRIER_FL_NUM,ORIGIN,DEST,CRS_DEP_TIME,DEP_TIME,DEP_DELAY,CRS_ARR_TIME,ARR_TIME,ARR_DELAY
> ```

---

## 🐍 3. JupyterLab: PySpark-заготовка

Создай ноутбук в JupyterLab и вставь следующий код. Он настроен на работу с MinIO через S3A.

> ⚠️ Убедись, что твой Spark-контейнер **доступен из Jupyter** (либо Jupyter внутри того же Docker Compose, либо Spark запущен в standalone-режиме и Jupyter подключается к нему).

### 🔧 Заготовка: `flights_etl.ipynb`

```python
# 1. Импорты
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat, lit, hour, to_timestamp
from pyspark.sql.types import IntegerType

# 2. Настройки MinIO (измени, если у тебя другие!)
minio_endpoint = "minio:9000"  # имя сервиса в docker-compose
access_key = "minioadmin"
secret_key = "minioadmin"

# 3. Создаём SparkSession с поддержкой S3A
spark = SparkSession.builder \
    .appName("Flights ETL") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_endpoint}") \
    .config("spark.hadoop.fs.s3a.access.key", access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .getOrCreate()

# 4. Читаем CSV из MinIO
input_path = "s3a://raw-flights-csv/2023-01.csv"
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path)

print("Схема данных:")
df.printSchema()
print(f"Количество строк: {df.count()}")

# 5. Очистка и обогащение
df_clean = df \
    .filter(col("ORIGIN").isNotNull() & col("DEST").isNotNull()) \
    .filter(col("DEP_DELAY").isNotNull() & col("ARR_DELAY").isNotNull()) \
    .withColumn("IS_DELAYED", when(col("ARR_DELAY") > 15, 1).otherwise(0)) \
    .withColumn("ROUTE", concat(col("ORIGIN"), lit("-"), col("DEST"))) \
    .withColumn("DEP_HOUR", (col("DEP_TIME") / 100).cast(IntegerType()))

# Покажем пример
df_clean.select("ORIGIN", "DEST", "ARR_DELAY", "IS_DELAYED", "ROUTE", "DEP_HOUR").show(5)

# 6. Сохраняем в Parquet в MinIO
output_path = "s3a://processed-flights/flights_2023_01.parquet"
df_clean.write \
    .mode("overwrite") \
    .parquet(output_path)

print(f"✅ Данные сохранены в {output_path}")

# 7. (Опционально) Сохранить в ORC
# df_clean.write.mode("overwrite").orc("s3a://processed-flights/flights_2023_01.orc")
```

---

## 🐳 4. Проверь зависимости

Убедись, что в твоём Spark-образе (или в Jupyter, если Spark локальный) есть:

- `hadoop-aws`
- `aws-java-sdk-bundle`

Если используешь официальный образ Spark — добавь в `docker-compose.yml` для Spark:

```yaml
environment:
  - SPARK_SUBMIT_OPTS=-Dspark.jars.ivy=/tmp/.ivy
```

Или собери кастомный образ с нужными JAR-файлами.

> 💡 Альтернатива: скачай JAR-файлы вручную и укажи их через `.config("spark.jars", "/opt/spark/jars/hadoop-aws-3.3.4.jar,...")`.

---

## ▶️ 5. Как запускать

1. Запусти весь стенд: `docker-compose up -d`
2. Зайди в JupyterLab (обычно `http://localhost:8888`)
3. Создай новый Python 3 notebook
4. Вставь код выше
5. Запусти ячейки по очереди

Если всё настроено — ты увидишь схему, количество строк и сообщение об успешной записи в MinIO.

---

## 🧩 Что дальше?

Когда это заработает:
- Добавь больше файлов (2023-02.csv и т.д.)
- Измени чтение на `s3a://raw-flights-csv/*.csv`
- Попробуй сохранить в **PostgreSQL** (дам пример отдельно)
- Создай DAG в Airflow, который запускает этот скрипт

---

Хочешь — могу сразу дать:
- Пример записи в PostgreSQL из Spark
- Пример DAG для Airflow
- Советы по настройке Spark в standalone-режиме с 2 воркерами

Скажи, что интересно дальше!

### USER
 - Пример записи в PostgreSQL из Spark
- Пример DAG для Airflow
- Советы по настройке Spark в standalone-режиме с 2 воркерами

### ASSISTANT
Отлично! Вот три полезные заготовки под твой стек:

---

## 1️⃣ **Пример записи данных из Spark в PostgreSQL**

Предположим, у тебя уже есть обработанный `DataFrame` `df_clean` (из предыдущего шага).  
Ты хочешь сохранить агрегированные данные (например, топ-10 маршрутов) в PostgreSQL.

### 🔧 Код (добавь в Jupyter или Spark-скрипт):

```python
# Агрегация: топ-10 самых задерживаемых маршрутов
top_routes = df_clean.groupBy("ROUTE") \
    .agg(
        {"IS_DELAYED": "sum", "ARR_DELAY": "avg"}
    ) \
    .withColumnRenamed("sum(IS_DELAYED)", "total_delays") \
    .withColumnRenamed("avg(ARR_DELAY)", "avg_delay") \
    .orderBy(col("total_delays").desc()) \
    .limit(10)

# Параметры подключения к PostgreSQL
pg_url = "jdbc:postgresql://postgres-db:5432/learn_base"  # имя сервиса из docker-compose
pg_properties = {
    "user": "airflow",        # или создай отдельного пользователя
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

# Запись в таблицу
top_routes.write \
    .mode("overwrite") \
    .option("truncate", "true") \
    .jdbc(url=pg_url, table="top_delayed_routes", properties=pg_properties)

print("✅ Таблица top_delayed_routes записана в PostgreSQL")
```

### ⚠️ Важно:
- Убедись, что в контейнере с Spark (или Jupyter) установлен **PostgreSQL JDBC драйвер**.
- Скачай его: [https://jdbc.postgresql.org/download/](https://jdbc.postgresql.org/download/postgresql-42.7.3.jar)
- Положи в папку, например: `./spark/jars/postgresql-42.7.3.jar`
- И добавь в `docker-compose.yml` для Spark/Jupyter:

```yaml
volumes:
  - ./spark/jars:/opt/spark/jars
```

Теперь Spark сможет писать в PostgreSQL.

---

## 2️⃣ **Пример DAG в Airflow для запуска Spark-задачи**

Предположим, твой Spark работает в **standalone-режиме**, и у тебя есть скрипт `/opt/airflow/dags/spark_flights_etl.py`.

### 📁 Файл: `dags/spark_flights_dag.py`

```python
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'retries': 1,
}

dag = DAG(
    'flights_etl_spark',
    default_args=default_args,
    description='ETL авиаперелётов через Spark',
    schedule_interval='@daily',
    catchup=False,
)

spark_task = SparkSubmitOperator(
    task_id='run_spark_etl',
    application='/opt/airflow/dags/spark_flights_etl.py',  # путь внутри airflow-webserver/scheduler
    conn_id='spark_default',  # должен быть настроен в Airflow UI!
    conf={
        'spark.executor.memory': '2g',
        'spark.executor.cores': '1',
        'spark.cores.max': '2',  # максимум ядер для всего приложения
        'spark.sql.adaptive.enabled': 'true'
    },
    dag=dag,
)
```

### 🔧 Настройка подключения в Airflow UI:
1. Зайди в Airflow → Admin → Connections
2. Создай новое подключение:
   - **Conn Id**: `spark_default`
   - **Conn Type**: `Spark`
   - **Host**: `spark://spark-master:7077` (если у тебя master называется `spark-master`)
   - Остальное оставь пустым

> 💡 Убедись, что `spark-master` и `spark-worker` доступны из контейнера Airflow по имени сервиса.

---

## 3️⃣ **Советы: Spark Standalone с 2 воркерами в Docker**

Добавь в свой `docker-compose.yml` следующие сервисы:

```yaml
  spark-master:
    image: bitnami/spark:3.5
    container_name: spark-master
    ports:
      - "7077:7077"
      - "8080:8080"
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    networks:
      - data-eng-net

  spark-worker-1:
    image: bitnami/spark:3.5
    container_name: spark-worker-1
    depends_on:
      - spark-master
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=2g
      - SPARK_WORKER_CORES=1
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    networks:
      - data-eng-net

  spark-worker-2:
    image: bitnami/spark:3.5
    container_name: spark-worker-2
    depends_on:
      - spark-master
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=2g
      - SPARK_WORKER_CORES=1
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    networks:
      - data-eng-net
```

### ✅ Что это даёт:
- Master на `spark://spark-master:7077`
- 2 воркера по 1 ядру и 2 ГБ RAM → Spark сможет параллельно обрабатывать данные
- Веб-интерфейс Spark: `http://localhost:8080` — там видно активные задачи

### 💡 Советы по производительности:
- В PySpark-скрипте используй `.repartition(4)` или `.coalesce()` для балансировки партиций.
- Читай CSV с `option("multiline", "false")` — иначе будет медленно.
- Используй **Parquet/ORC** для промежуточных данных — они в 3–5 раз быстрее CSV.

---

## 🧪 Как проверить, что всё работает?

1. Запусти `docker-compose up -d`
2. Зайди в Airflow → запусти DAG вручную
3. Открой Spark UI (`http://localhost:8080`) — должен появиться running application
4. Проверь MinIO — появились ли Parquet-файлы?
5. Подключись к PostgreSQL:  
   ```sql
   SELECT * FROM top_delayed_routes;
   ```

---

Если хочешь — могу:
- Дать полный `spark_flights_etl.py` (автономный скрипт)
- Помочь с настройкой JDBC в Jupyter
- Подсказать, как добавить Spark History Server (чтобы видеть завершённые задачи)

Готов помочь на любом этапе!