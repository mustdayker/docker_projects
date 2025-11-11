from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def main():
    print("🚀 Starting test Spark application...")

    # Простая сессия без лишних конфигов
    spark = SparkSession.builder \
        .appName("airflow-test-app") \
        .getOrCreate()

    # Создаем тестовый датафрейм
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("value", IntegerType(), True)
    ])

    data = [
        (1, "Alice", 100),
        (2, "Bob", 200),
        (3, "Charlie", 300),
        (4, "David", 400),
        (5, "Eve", 500)
    ]

    df = spark.createDataFrame(data, schema=schema)

    print("✅ Spark session created successfully!")
    print("📊 Test DataFrame:")
    df.show()

    # Простая агрегация для демонстрации
    result = df.groupBy().sum("value").collect()
    total_value = result[0][0]
    print(f"💰 Total value: {total_value}")

    print("✅ Spark application completed successfully!")


if __name__ == "__main__":
    main()