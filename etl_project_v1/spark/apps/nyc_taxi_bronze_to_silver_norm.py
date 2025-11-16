from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql import SparkSession
import re
from minio import Minio
from minio.error import S3Error
import time

import ast
import argparse

# Конфигурация MinIO
MINIO_ENDPOINT = 'minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'


def get_minio_client():
    """Создает и возвращает клиент MinIO"""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )


def extract_month_from_filename(file_path):
    """Извлекает месяц из имени файла в формате YYYY-MM"""
    match = re.search(r'(\d{4}-\d{2})', file_path)
    return match.group(1) if match else None


def get_processed_slices(output_bucket, output_prefix):
    """Возвращает список уже обработанных срезов из выходного бакета используя MinIO"""
    try:
        client = get_minio_client()
        processed_slices = set()

        objects = client.list_objects(output_bucket, prefix=output_prefix, recursive=True)

        for obj in objects:
            month = extract_month_from_filename(obj.object_name)
            if month:
                processed_slices.add(month)

        print(f"📁 Найдено обработанных срезов в {output_bucket}/{output_prefix}: {len(processed_slices)}")
        return processed_slices

    except S3Error as e:
        if e.code == 'NoSuchBucket':
            print(f"⚠️ Бакет {output_bucket} не существует или пустой")
        else:
            print(f"⚠️ Ошибка при чтении {output_bucket} бакета: {e}")
        return set()
    except Exception as e:
        print(f"⚠️ Не удалось прочитать {output_bucket} бакет: {e}")
        return set()


def get_input_files_with_months(input_bucket, input_prefix):
    """Возвращает список файлов/папок из входного бакета с извлеченными месяцами используя MinIO"""
    try:
        client = get_minio_client()
        input_files = []

        objects = client.list_objects(input_bucket, prefix=input_prefix, recursive=False)

        for obj in objects:
            object_name = obj.object_name

            is_parquet_file = object_name.endswith('.parquet')
            is_folder = not is_parquet_file and object_name.endswith('/')

            if is_parquet_file or is_folder:
                month = extract_month_from_filename(object_name)
                if month:
                    s3_path = f"s3a://{input_bucket}/{object_name}"
                    input_files.append({
                        'path': s3_path,
                        'month': month,
                        'file_name': object_name.split('/')[-1] if is_parquet_file else object_name.split('/')[-2] + '/'
                    })

        print(f"📁 Найдено объектов в {input_bucket}/{input_prefix}: {len(input_files)}")
        return input_files

    except Exception as e:
        print(f"❌ Ошибка при чтении {input_bucket} бакета: {e}")
        return []


def standardize_nyc_taxi_data(spark, input_path, output_path):
    """Стандартизирует данные NYC Taxi"""
    output_path = output_path.replace('.parquet', '')

    df = spark.read.parquet(input_path)

    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.lower())

    type_mapping = {
        "vendorid": IntegerType(),
        "pulocationid": IntegerType(),
        "dolocationid": IntegerType(),
        "payment_type": IntegerType(),
        "ratecodeid": IntegerType(),
        "passenger_count": IntegerType(),
        "fare_amount": DoubleType(),
        "extra": DoubleType(),
        "mta_tax": DoubleType(),
        "tip_amount": DoubleType(),
        "tolls_amount": DoubleType(),
        "improvement_surcharge": DoubleType(),
        "total_amount": DoubleType(),
        "congestion_surcharge": DoubleType(),
        "airport_fee": DoubleType(),
        "cbd_congestion_fee": DoubleType(),
        "trip_distance": DoubleType()
    }

    for col_name, target_type in type_mapping.items():
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.coalesce(
                    F.col(col_name).cast(target_type),
                    F.lit(0 if target_type == IntegerType() else 0.0)
                )
            )

    expected_columns = [
        "vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "ratecodeid", "store_and_fwd_flag",
        "pulocationid", "dolocationid", "payment_type", "fare_amount", "extra",
        "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
        "total_amount", "congestion_surcharge", "airport_fee", "cbd_congestion_fee"
    ]

    final_columns = [col for col in expected_columns if col in df.columns]
    df_standardized = df.select(final_columns)

    (df_standardized
     .coalesce(1)
     .write
     .mode("overwrite")
     .option("compression", "snappy")
     .parquet(output_path)
     )

    print(f"✅ Стандартизировано: {input_path} -> {output_path}")
    return df_standardized


def process_incremental_nyc_taxi_files(spark, input_bucket, input_prefix, output_bucket, output_prefix):
    """Обрабатывает только новые файлы NYC Taxi из входного бакета в выходной"""

    processed_slices = get_processed_slices(output_bucket, output_prefix)
    input_files = get_input_files_with_months(input_bucket, input_prefix)

    new_files = [f for f in input_files if f['month'] not in processed_slices]

    print(f"📊 Статистика:")
    print(f"   - Всего во входном бакете: {len(input_files)}")
    print(f"   - Уже в выходном бакете: {len(processed_slices)}")
    print(f"   - Новых для обработки: {len(new_files)}")

    if not new_files:
        print("🎉 Все срезы уже обработаны! Ничего делать не нужно.")
        return

    for i, file_info in enumerate(new_files, 1):
        input_path = file_info['path']
        file_name = file_info['file_name']

        output_path = f"s3a://{output_bucket}/{output_prefix}{file_name}".replace('.parquet', '')

        print(f"🔄 Обрабатываю новый срез ({i}/{len(new_files)}): {file_info['month']}")

        try:
            standardize_nyc_taxi_data(spark, input_path, output_path)
            print(f"✅ Успешно обработан: {file_info['month']}")
            print()
        except Exception as e:
            print(f"❌ Ошибка при обработке {file_info['month']}: {e}")
            raise

    print(f"🎉 Обработка завершена! Обработано {len(new_files)} новых срезов.")



def main():
    """Основная функция Spark приложения"""

    # Создаем парсер аргументов
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-data', type=str, required=True)
    parser.add_argument('--execution-date', type=str, required=True)

    # Парсим аргументы
    args = parser.parse_args()

    # Теперь используем полученные значения
    input_data = args.input_data
    execution_date = args.execution_date

    print("=" * 60)
    print(f"INPUT DATA FROM XCOM: {input_data}")
    print(f"EXECUTION DATE: {execution_date}")
    print("=" * 60)


    print("-------- 📊 Статус задачи download_nyc_taxi_data ---------")

    # Парсим Python dict строку
    try:
        input_dict = ast.literal_eval(input_data)
    except (SyntaxError, ValueError) as e:
        print(f"❌ Ошибка парсинга: {e}")
        print(f"Полученная строка: {repr(input_data)}")
        raise

    for i in input_dict.items():
        print(i)

    print("----------------------------------------------------------")
    print("\n\n")
    start_time = time.time()

    spark = SparkSession.builder \
        .appName("nyc-taxi-normalisation") \
        .getOrCreate()

    execution_time = time.time() - start_time
    print(f"⏱️  Spark сессия стартовала за: {execution_time:.2f} секунд ({execution_time / 60:.2f} минут)")

    # Устанавливаем уровень логгирования для Spark
    spark.sparkContext.setLogLevel("WARN")  # или "ERROR"

    # Устанавливаем уровень логгирования для Py4J (библиотека для связи Python-Java)
    logger = spark.sparkContext._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.WARN)
    logger.LogManager.getLogger("io").setLevel(logger.Level.WARN)

    start_time = time.time()

    print()
    try:
        process_incremental_nyc_taxi_files(
            spark=spark,
            input_bucket='bronze',
            input_prefix='nyc-taxi-data/',
            output_bucket='silver',
            output_prefix='nyc-taxi-data-norm/'
        )

        execution_time = time.time() - start_time

        print()
        print("🎊 Приложение успешно завершило работу!")
        print(f"⏱️  Время выполнения ETL процесса: {execution_time:.2f} секунд ({execution_time / 60:.2f} минут)")

        print("\n\n")
    except Exception as e:
        print(f"💥 Критическая ошибка в приложении: {e}")
        print("\n\n\n")
        raise


if __name__ == "__main__":
    main()