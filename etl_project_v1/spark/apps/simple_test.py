#!/usr/bin/env python3
"""
Простой тест Spark приложения - проверяет что Spark сессия работает
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    print("=== Starting Simple Spark Test ===")

    # Список драйверов
    drivers = [
        "/opt/spark/jars/hadoop-aws-3.3.4.jar",
        "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
        "/opt/spark/jars/wildfly-openssl-1.0.7.Final.jar",
        "/opt/spark/jars/postgresql-42.6.0.jar",
    ]

    print("Creating Spark session...")

    # Создание Spark сессии
    spark = (SparkSession.builder
             .appName("simple-spark-test")
             .master("spark://spark-master:7077")
             .config("spark.jars", ",".join(drivers))
             .getOrCreate()
             )

    try:
        print("Spark session created successfully!")
        print(f"Spark version: {spark.version}")
        print(f"Master: {spark.conf.get('spark.master')}")

        # Простейшая операция - создаем маленький DataFrame
        print("Creating test DataFrame...")
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        columns = ["name", "age"]

        df = spark.createDataFrame(data, columns)

        print("DataFrame schema:")
        df.printSchema()

        print("DataFrame content:")
        df.show()

        # Простая агрегация
        print("Simple aggregation...")
        result = df.agg(F.avg("age").alias("average_age"))

        print("Aggregation result:")
        result.show()

        # Проверка количества записей
        count = df.count()
        print(f"Total records: {count}")

        print("=== Spark Test Completed Successfully! ===")

    except Exception as e:
        print(f"ERROR in Spark job: {str(e)}")
        raise e
    finally:
        print("Stopping Spark session...")
        spark.stop()
        print("Spark session stopped.")


if __name__ == "__main__":
    main()