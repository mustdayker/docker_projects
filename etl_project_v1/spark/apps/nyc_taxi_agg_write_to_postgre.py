from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import time


def main(write_table):
    """Основная функция Spark приложения"""

    print("\n\n")


    spark = SparkSession.builder \
        .appName("nyc-taxi-agg-and-write") \
        .getOrCreate()


    # Устанавливаем уровень логгирования для Spark
    spark.sparkContext.setLogLevel("WARN")  # или "ERROR"

    # Устанавливаем уровень логгирования для Py4J (библиотека для связи Python-Java)
    logger = spark.sparkContext._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.WARN)
    logger.LogManager.getLogger("io").setLevel(logger.Level.WARN)

    print()
    # ------------------ Читаем весь датафрейм ------------------

    start_time = time.time()

    df = spark.read.parquet("s3a://silver/nyc-taxi-data-eda/*")  # yellow_tripdata_2025-09/")

    print(f"Общий размер датасета: {df.count()} строк.")

    print("Срезы датасета:")
    df.groupBy("date_month", "year", "month").count().orderBy("year", "month").show(50)

    print("Схема датасета:")
    df.printSchema()

    execution_time = time.time() - start_time
    print(f"⏱️  Прочитано за: {execution_time:.2f} секунд ({execution_time / 60:.2f} минут)")

    print()



    # ------------------ Собираем агрегаты ------------------

    start_time = time.time()
    try:
        print("Собираем агрегаты")
        print()

        df_agg = df.groupBy(
            "date_month",
            "year",
            "month",
            "day_of_week",
            "time_of_day",
            "pulocationid"
        ).agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_revenue"),
            F.avg("trip_duration_minutes").alias("avg_duration"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("avg_speed_kmh").alias("avg_speed"),
            F.avg("tip_ratio").alias("avg_tip_ratio"),
            F.avg(F.col("has_tip").cast("double")).alias("tip_probability"),
            F.avg("passenger_count").alias("avg_passengers"),
            F.avg("revenue_per_minute").alias("avg_efficiency")
        )

        print(f"Размер агрегированного датасета: {df_agg.count()} строк.")


        print("Предпросмотр датасета:")
        df_agg.show(5)

        execution_time = time.time() - start_time
        print(f"⏱️  Агрегаты собраны за: {execution_time:.2f} секунд ({execution_time / 60:.2f} минут)")

        print("\n\n")
    except Exception as e:
        print(f"💥 Критическая ошибка в приложении: {e}")
        print("\n\n\n")
        raise

    # ------------------ Пишем датасет в БД ------------------

    start_time = time.time()
    try:
        print(f"Таблица для записи: {write_table}")
        print()
        print("Пишем датасет в БД ...")
        print()


        (df_agg.write.format("jdbc")
         .option("url", "jdbc:postgresql://postgres-db:5432/learn_base")
         .option("driver", "org.postgresql.Driver")
         .option("user", "airflow")
         .option("password", "airflow")
         .option("dbtable", write_table)
         .option("batchsize", 10000)
         .mode("overwrite")
         .save())

        execution_time = time.time() - start_time
        print(f"⏱️  Датасет записан за: {execution_time:.2f} секунд ({execution_time / 60:.2f} минут)")

        print("\n\n")
    except Exception as e:
        print(f"💥 Критическая ошибка в приложении: {e}")
        print("\n\n\n")
        raise



if __name__ == "__main__":
    main(write_table="nyc_taxi.nyc_taxi_agg_table")