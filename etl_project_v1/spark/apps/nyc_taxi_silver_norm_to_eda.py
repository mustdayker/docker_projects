from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql import SparkSession
import re
from minio import Minio
from minio.error import S3Error
import time

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


def eda_nyc_taxi_data(spark, input_path, output_path):
    """
    Очищает данные NYC Taxi
    """
    df = spark.read.format("parquet").load(input_path)

    df_with_duration = df.withColumn(
        "trip_duration_minutes",
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60
    )

    # Создаем отфильтрованный датафрейм
    df_clean = df_with_duration.filter(
        # Временные аномалии: длительность больше 1 минуты и меньше верхней границы
        (F.col("passenger_count") >= 0) &
        (F.col("passenger_count") <= 6) &

        (F.col("trip_duration_minutes") > 1) &
        (F.col("trip_duration_minutes") < 90) &

        # Платежи
        (F.col("fare_amount") > 0) &
        (F.col("fare_amount") < 110) &
        (F.col("extra") >= 0) &
        (F.col("mta_tax") >= 0) &
        (F.col("improvement_surcharge") >= 0) &
        (F.col("tip_amount") >= 0) &
        (F.col("tip_amount") < 30) &
        (F.col("tolls_amount") >= 0) &
        (F.col("tolls_amount") < 30) &
        (F.col("total_amount") > 0) &
        (F.col("total_amount") < 110) &

        (F.col("trip_distance") > 0) &
        (F.col("trip_distance") < 100) &

        (F.year("tpep_pickup_datetime") >= 2022) &
        (F.year("tpep_pickup_datetime") < 2026)
        # Добавьте другие условия по необходимости
    )

    # Обогащаем

    df_clean = (df_clean
                .withColumn("date_month", F.date_trunc("month", "tpep_pickup_datetime"))
                .withColumn("year", F.year("tpep_pickup_datetime"))  # Год
                .withColumn("month", F.month("tpep_pickup_datetime"))  # Месяц
                .withColumn("day", F.dayofmonth("tpep_pickup_datetime"))
                .withColumn("day_of_week", F.dayofweek("tpep_pickup_datetime"))
                .withColumn("hour", F.hour("tpep_pickup_datetime"))
                .withColumn("is_weekend", F.when(F.dayofweek("tpep_pickup_datetime").isin(1, 7), 1).otherwise(0))
                .withColumn("time_of_day",
                            F.when((F.col("hour") >= 5) & (F.col("hour") < 12), "Утро")
                            .when((F.col("hour") >= 12) & (F.col("hour") < 17), "День")
                            .when((F.col("hour") >= 17) & (F.col("hour") < 21), "Вечер")
                            .otherwise("Ночь"))
                .withColumn("is_rush_hour",
                            F.when(
                                ((F.col("hour") >= 7) & (F.col("hour") <= 10)) |  # Утренний пик
                                ((F.col("hour") >= 16) & (F.col("hour") <= 19)),  # Вечерний пик
                                1
                            ).otherwise(0))
                .withColumn("avg_speed_kmh",
                            F.when(F.col("trip_duration_minutes") > 0,
                                   (F.col("trip_distance") * 1.60934) / (F.col("trip_duration_minutes") / 60.0)
                                   ).otherwise(None))
                .withColumn("tip_ratio", F.col("tip_amount") / F.col("fare_amount"))
                .withColumn("has_tip", F.when(F.col("tip_amount") > 0, 1).otherwise(0))  # Бинарный целевой признак
                .withColumn("revenue_per_minute",
                            F.when(F.col("trip_duration_minutes") > 0,
                                   F.col("total_amount") / F.col("trip_duration_minutes")
                                   ).otherwise(None))
                )
    # Удаляем поля с большим количеством пропусков
    df_clean = df_clean.drop("store_and_fwd_flag")

    print(f"Исходный размер: {df.count()}")
    print(f"Размер после очистки: {df_clean.count()}")
    print(f"Удалено {df.count() - df_clean.count()} строк ({(1 - df_clean.count() / df.count()) * 100:.2f}%)")

    # 5. Сохраняем с оптимальными настройками
    (df_clean
     .coalesce(1)
     .write
     .mode("overwrite")
     .option("compression", "snappy")
     .parquet(output_path)
     )

    print(f"✅ Стандартизировано: {input_path} -> {output_path}")
    return df_clean


def eda_incremental_nyc_taxi_files(spark, input_bucket, input_prefix, output_bucket, output_prefix):
    """Обрабатывает только новые файлы NYC Taxi из входного бакета в выходной"""

    # Получаем списки обработанных и доступных файлов через MinIO
    processed_slices = get_processed_slices(output_bucket, output_prefix)
    input_files = get_input_files_with_months(input_bucket, input_prefix)

    # Фильтруем только новые файлы
    new_files = [f for f in input_files if f['month'] not in processed_slices]

    print(f"📊 Статистика:")
    print(f"   - Всего во входном бакете: {len(input_files)}")
    print(f"   - Уже в выходном бакете: {len(processed_slices)}")
    print(f"   - Новых для обработки: {len(new_files)}")

    if not new_files:
        print("🎉 Все срезы уже обработаны! Ничего делать не нужно.")
        return



    # Обрабатываем только новые файлы
    for i, file_info in enumerate(new_files, 1):
        input_path = file_info['path']
        file_name = file_info['file_name']

        # Формируем выходной путь, сохраняя структуру после префикса
        # Пример: входной путь s3a://bronze/nyc-taxi-data/yellow_tripdata_2022-01
        # Выходной путь: s3a://silver/nyc-taxi-data/yellow_tripdata_2022-01
        output_path = f"s3a://{output_bucket}/{output_prefix}{file_name}".replace('.parquet', '')

        print(f"🔄 Обрабатываю новый срез ({i}/{len(new_files)}): {file_info['month']}")

        try:
            eda_nyc_taxi_data(spark, input_path, output_path)
            print(f"✅ Успешно обработан: {file_info['month']}")
            print()
        except Exception as e:
            print(f"❌ Ошибка при обработке {file_info['month']}: {e}")

    print(f"🎉 Обработка завершена! Обработано {len(new_files)} новых срезов.")


def main():
    """Основная функция Spark приложения"""

    print("\n\n")
    start_time = time.time()

    spark = SparkSession.builder \
        .appName("nyc-taxi-eda-ready") \
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
        eda_incremental_nyc_taxi_files(
            spark=spark,
            input_bucket='silver',
            input_prefix='nyc-taxi-data-norm/',
            output_bucket='silver',
            output_prefix='nyc-taxi-data-eda/'
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