from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta
from minio import Minio
from minio.error import S3Error
import requests
from tqdm import tqdm
import os
import tempfile


# -------------------- Функции из вашего скрипта --------------------

def get_available_remote_files(base_url, filename_template, year):
    """Проверить какие файлы фактически существуют на сайте"""
    available_files = []

    print("🔍 Проверка доступных файлов на сайте...")

    for month in tqdm(range(1, 13), desc="Проверка месяца"):
        filename = filename_template.format(year=year, month=month)
        url = f"{base_url}/{filename}"

        try:
            response = requests.head(url, timeout=10)
            if response.status_code == 200:
                available_files.append(filename)
                print(f"  ✓ {filename} - доступен")
            else:
                print(f"  ✗ {filename} - недоступен (код: {response.status_code})")

        except requests.exceptions.RequestException as e:
            print(f"  ✗ {filename} - ошибка: {e}")

    return available_files


def get_local_minio_files(minio_client, bucket_name, prefix):
    """Получить список файлов в MinIO"""
    local_files = []
    try:
        objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
        for obj in objects:
            filename = obj.object_name.replace(f"{prefix}/", "")
            local_files.append(filename)
    except S3Error as e:
        print(f"Ошибка при чтении бакета: {e}")

    return local_files


def download_missing_files(**kwargs):
    """Загрузка только отсутствующих файлов в MinIO"""

    # Параметры из op_kwargs
    bucket_name = kwargs.get('bucket_name', 'bronze')
    prefix = kwargs.get('prefix', 'nyc-taxi-data')
    base_url = kwargs.get('base_url', 'https://d37ci6vzurychx.cloudfront.net/trip-data')
    filename_template = kwargs.get('filename_template', 'yellow_tripdata_{year}-{month:02d}.parquet')

    # Год из execution context через Jinja
    execution_year = kwargs.get('year')
    if not execution_year:
        # Если год не передан, используем текущий год из execution_date
        execution_date = kwargs['execution_date']
        execution_year = execution_date.year

    print(f"🎯 Обрабатываем год: {execution_year}")

    # Настройка клиента MinIO
    minio_client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    # Создаем бакет если нужно
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            print(f"✓ Бакет {bucket_name} создан")
    except S3Error as e:
        return [f"✗ Ошибка бакета: {e}"]

    # Получаем списки файлов
    remote_files = get_available_remote_files(base_url, filename_template, execution_year)
    local_files = get_local_minio_files(minio_client, bucket_name, prefix)

    # Находим отсутствующие файлы
    missing_files = list(set(remote_files) - set(local_files))

    # Блок статистики
    print(f"\n📊 СТАТИСТИКА:")
    print(f"• Загружено в MinIO: {len(local_files)} файл(ов)")
    print(f"• Доступно на сайте: {len(remote_files)} файл(ов)")

    for file in sorted(remote_files):
        print(f"     - {file}")

    print()
    if missing_files:
        print(f"• Из них отсутствует в MinIO: {len(missing_files)} файл(ов)")
        for file in sorted(missing_files):
            print(f"     - {file}")

    if not missing_files:
        print("✅ Все доступные файлы уже загружены")
        return {"status": "success", "message": "Все файлы уже загружены", "downloaded_files": []}

    results = []
    downloaded_files = []

    # Скачиваем только отсутствующие файлы
    for filename in tqdm(missing_files, desc="Загрузка недостающих"):
        url = f"{base_url}/{filename}"

        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()

            # Создаем временный файл
            with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as temp_file:
                temp_path = temp_file.name

                # Скачиваем файл на диск
                total_size = int(response.headers.get('content-length', 0))
                for chunk in response.iter_content(chunk_size=8192 * 8):
                    if chunk:
                        temp_file.write(chunk)

            # Получаем реальный размер файла
            file_size = os.path.getsize(temp_path)

            # Загружаем в MinIO
            minio_client.fput_object(
                bucket_name=bucket_name,
                object_name=f"{prefix}/{filename}",
                file_path=temp_path
            )

            # Удаляем временный файл
            os.unlink(temp_path)

            result_msg = f"✓ {filename} ({file_size / (1024 * 1024):.1f} MB)"
            results.append(result_msg)
            downloaded_files.append(filename)
            print(result_msg)

        except Exception as e:
            # Удаляем временный файл в случае ошибки
            if 'temp_path' in locals():
                try:
                    os.unlink(temp_path)
                except:
                    pass
            error_msg = f"✗ {filename}: {e}"
            results.append(error_msg)
            print(error_msg)

    return {
        "status": "success" if downloaded_files else "partial_success",
        "message": f"Загружено {len(downloaded_files)} из {len(missing_files)} файлов",
        "downloaded_files": downloaded_files,
        "details": results
    }



# -------------------- Настройка DAG --------------------
# -------------------- Настройка DAG --------------------
# -------------------- Настройка DAG --------------------
# -------------------- Настройка DAG --------------------
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


    download_nyc_taxi_data = PythonOperator(
        task_id='download_nyc_taxi_data',
        python_callable=download_missing_files,
        op_kwargs={
            'bucket_name': 'bronze',
            'prefix': 'nyc-taxi-data',
            'base_url': 'https://d37ci6vzurychx.cloudfront.net/trip-data',
            'filename_template': 'yellow_tripdata_{year}-{month:02d}.parquet',
            # 'year': 2024,
            # Год будет автоматически подставляться через {{ data_interval_end.year }}
        },
        provide_context=True,  # Передаем execution context для доступа к execution_date
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
        conn_id='spark_cluster',
        jars=','.join(spark_drivers),
        name='airflow-distributed-test',
        verbose=True,
        retries=0
    )


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

    # Здесь в будущем можно добавить следующие таски:
    # - data_cleaning_task
    # - data_aggregation_task
    # - load_to_postgres_task
    # - update_superset_dashboard_task

    (
            download_nyc_taxi_data >>
            bronze_to_silver_norm >>
            silver_norm_to_eda >>
            agg_write_to_postgres
     )

# Документация DAG
dag.doc_md = """
## NYC Taxi Data Pipeline

Этот DAG загружает данные NYC Taxi из публичного источника в MinIO.

### Задачи:
1. **download_nyc_taxi_data** - Загружает отсутствующие файлы данных такси NYC за текущий год

### Параметры:
- Автоматически определяет год из execution_date
- Проверяет какие файлы уже есть в MinIO
- Скачивает только отсутствующие файлы
- Сохраняет в бакет `bronze` с префиксом `nyc-taxi-data`

### Расписание:
- Запускается ежемесячно (@monthly)
"""