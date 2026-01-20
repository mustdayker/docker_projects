from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta
from minio import Minio
from minio.error import S3Error

# Таски для PythonOperator
# Лежит в \airflow\tasks\nyc_taxi\nyc_download.py
# 1 таска - 1 функция
from tasks.nyc_taxi.nyc_download import (
    get_available_remote_files,
    get_local_minio_files,
    download_missing_files
)


# -------------------- Настройка DAG --------------------


default_args = {
    'owner': 'mustdayker',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'nyc_taxi_data_pipeline',
        default_args=default_args,
        description='Пайплайн для загрузки и обработки данных NYC Taxi',
        schedule_interval='@monthly',  # Запускается ежемесячно
        start_date=datetime(2024, 1, 1),  # Начинаем с 1 января 2024
        catchup=False,  # Не запускать пропущенные даги
        tags=['nyc_taxi', 'data_pipeline'],
) as dag:

    # -------------------  ТАСКИ ------------------------
    # -------------------  ТАСКИ ------------------------
    # -------------------  ТАСКИ ------------------------

    bucket_name = 'bronze'
    prefix = 'nyc-taxi-data'
    base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
    filename_template = 'yellow_tripdata_{year}-{month:02d}.parquet'


    # Смотрим какие срезы есть в наличии за указанный год
    remote_files_task = PythonOperator(
        task_id='remote_files_task',
        python_callable=lambda **context: get_available_remote_files(
            base_url=base_url,
            filename_template=filename_template,
            # year=context['execution_date'].year,
            year=2025,
        )
    )

    local_files_task = PythonOperator(
        task_id='local_files_task',
        python_callable=lambda **context: get_local_minio_files(
            bucket_name=bucket_name,
            prefix=prefix,
        )
    )

    download_nyc_taxi_data = PythonOperator(
        task_id='download_nyc_taxi_data',
        python_callable=lambda **context: download_missing_files(
            bucket_name=bucket_name,
            prefix=prefix,
            base_url=base_url,
            filename_template=filename_template,
            remote_files=context['ti'].xcom_pull(task_ids='remote_files_task'),
            local_files=context['ti'].xcom_pull(task_ids='local_files_task'),
            # year=context['execution_date'].year,
        )
    )

    spark_drivers = [
        "/opt/spark/external-jars/hadoop-aws-3.3.4.jar",
        "/opt/spark/external-jars/aws-java-sdk-bundle-1.12.262.jar",
        "/opt/spark/external-jars/wildfly-openssl-1.0.7.Final.jar",
        "/opt/spark/external-jars/postgresql-42.6.0.jar",
    ]


    bronze_to_silver_norm = SparkSubmitOperator(
        task_id='bronze_to_silver_norm',
        application='/opt/spark/apps/nyc_taxi_bronze_to_silver_norm.py',
        application_args=[
            "--input-data", "{{ ti.xcom_pull(task_ids='download_nyc_taxi_data') }}",
            "--execution-date", "{{ ds }}"  # 2024-01-15
        ],
        conn_id='spark_cluster',
        jars=','.join(spark_drivers),
        name='airflow-distributed-test',
        verbose=True,
        retries=0
    )
    #             "--input-data", "{{ ti.xcom_pull(task_ids='download_nyc_taxi_data') | tojson}}",


    silver_norm_to_eda = SparkSubmitOperator(
        task_id='silver_norm_to_eda',
        application='/opt/spark/apps/nyc_taxi_silver_norm_to_eda.py',
        conn_id='spark_cluster',
        jars=','.join(spark_drivers),
        name='airflow-distributed-test',
        verbose=True,
        retries=0
    )

    agg_write_to_postgres = SparkSubmitOperator(
        task_id='agg_write_to_postgres',
        application='/opt/spark/apps/nyc_taxi_agg_write_to_postgre.py',
        conn_id='spark_cluster',
        jars=','.join(spark_drivers),
        name='airflow-distributed-test',
        verbose=True,
        retries=0
    )

    (
        [remote_files_task, local_files_task]
        >> download_nyc_taxi_data
        >> bronze_to_silver_norm
        >> silver_norm_to_eda
        >> agg_write_to_postgres
    )

# Документация DAG
dag.doc_md = """
## NYC Taxi Data Pipeline

Этот DAG загружает данные NYC Taxi из публичного источника в MinIO.

### Задачи:
1. **`remote_files_task`** - Получает список доступных файлов за год запуска DAG
2. **`local_files_task`** - Получает список доступных файлов в хранилище
3. **`download_nyc_taxi_data`** - Скачивает недостающие файлы в MinIO://bronze
4. **`bronze_to_silver_norm`** - Нормализует файлы из слоя bronze и кладет в слой silver
5. **`silver_norm_to_eda`** - Берет нормализованные данные из silver, чистит и обогощает
6. **`agg_write_to_postgres`** - Создает агрегаты, записывает результаты в БД Postgres


### Расписание:
- Запускается ежемесячно (@monthly)
"""