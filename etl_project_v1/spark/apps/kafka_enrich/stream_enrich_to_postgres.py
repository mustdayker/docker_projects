#!/usr/bin/env python
"""
Spark Structured Streaming приложение.
Читает данные из Kafka, обогащает их из Parquet справочников в MinIO,
и записывает в PostgreSQL.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация подключений
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "user-events"
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "library"
POSTGRES_URL = "jdbc:postgresql://postgres-db:5432/learn_base"
POSTGRES_PROPERTIES = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified"  # Для правильной обработки дат
}


def create_spark_session():
    """Создание Spark сессии с необходимыми конфигурациями"""
    return (SparkSession.builder
            .appName("KafkaEnrichToPostgres")
            .config("spark.sql.shuffle.partitions", 10)  # Для учебных целей
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate())


def read_kafka_stream(spark):
    """Чтение потока из Kafka"""
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "latest")  # Читаем только новые сообщения
            .option("failOnDataLoss", "false")  # Не падаем, если данные потеряны
            .load())


def parse_json_data(df):
    """Парсинг JSON из Kafka и приведение к нужным типам"""
    # Схема входящих данных
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("date", StringType(), True),  # Сначала строка, потом конвертируем
        StructField("event_date", StringType(), True),  # Сначала строка, потом конвертируем
        StructField("event", IntegerType(), True),
        StructField("username", StringType(), True),
        StructField("group", IntegerType(), True),
        StructField("value", DoubleType(), True)
    ])

    # Парсим JSON и конвертируем строки в правильные типы
    parsed = (df
              .select(from_json(col("value").cast("string"), schema).alias("data"))
              .select("data.*")
              .withColumn("date", col("date").cast(DateType()))
              .withColumn("event_date", col("event_date").cast(TimestampType())))

    return parsed


def read_dimension_tables(spark):
    """Чтение справочников из MinIO"""
    # Читаем справочник событий
    events_df = (spark.read
                 .format("parquet")
                 .option("header", "true")
                 .load(f"s3a://{MINIO_BUCKET}/kafka-enrich/event_list.parquet")
                 .withColumnRenamed("id", "event_id")
                 .withColumnRenamed("event", "event_name"))

    # Читаем справочник групп
    groups_df = (spark.read
                 .format("parquet")
                 .option("header", "true")
                 .load(f"s3a://{MINIO_BUCKET}/kafka-enrich/group_list.parquet")
                 .withColumnRenamed("id", "group_id")
                 .withColumnRenamed("group", "group_name"))

    # Кэшируем справочники, так как они маленькие
    events_df.cache().count()
    groups_df.cache().count()

    return events_df, groups_df


def enrich_data(stream_df, events_df, groups_df):
    """Обогащение данных через JOIN со справочниками"""
    # Сначала JOIN с событиями
    enriched = (stream_df
                .join(events_df,
                      stream_df.event == events_df.event_id,
                      "left")
                .drop("event_id")  # Убираем дублирующийся ID
                .withColumnRenamed("event_name", "event"))  # Переименовываем обратно

    # Затем JOIN с группами
    enriched = (enriched
                .join(groups_df,
                      enriched.group == groups_df.group_id,
                      "left")
                .drop("group_id")
                .withColumnRenamed("group_name", "group"))

    return enriched


def write_to_postgres(batch_df, batch_id):
    """Запись каждого микробатча в PostgreSQL"""
    try:
        # Выбираем и переименовываем колонки в соответствии с таблицей
        final_df = batch_df.select(
            col("id"),
            col("date"),
            col("event_date"),
            col("event"),
            col("username"),
            col("group"),
            col("value")
        )

        # Запись в PostgreSQL
        final_df.write \
            .mode("append") \
            .jdbc(url=POSTGRES_URL,
                  table="kafka_farm.user_events",
                  properties=POSTGRES_PROPERTIES)

        logger.info(f"Batch {batch_id}: успешно записано {final_df.count()} записей")
    except Exception as e:
        logger.error(f"Batch {batch_id}: ошибка записи - {str(e)}")


def main():
    """Основная функция приложения"""
    # Создаем Spark сессию
    spark = create_spark_session()
    logger.info("Spark сессия создана")

    # Читаем справочники
    events_df, groups_df = read_dimension_tables(spark)
    logger.info("Справочники загружены из MinIO")

    # Читаем поток из Kafka
    kafka_stream = read_kafka_stream(spark)
    logger.info("Подключение к Kafka установлено")

    # Парсим JSON
    parsed_stream = parse_json_data(kafka_stream)

    # Обогащаем данные
    enriched_stream = enrich_data(parsed_stream, events_df, groups_df)

    # Запускаем стриминг с записью в PostgreSQL
    query = (enriched_stream.writeStream
             .foreachBatch(write_to_postgres)
             .outputMode("append")
             .trigger(processingTime="10 seconds")  # Микробатчи каждые 10 секунд
             .option("checkpointLocation", "/tmp/spark-checkpoints/kafka-enrich")
             .start())

    logger.info("Стриминг запущен. Ожидание данных...")

    # Ожидаем завершения (по Ctrl+C)
    query.awaitTermination()


if __name__ == "__main__":
    main()