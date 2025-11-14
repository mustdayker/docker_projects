import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text


def get_df_from_postgres(
        query="""SELECT 1 AS one""",
        host="postgres-db",
        port=5432,
        user="airflow",
        password="airflow",
        database="learn_base",):

    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)

    with engine.connect() as connection:
        df = pd.read_sql_query(text(query), connection)
    return df


def ddl_on_postgres(
        query="""SELECT 1 AS one""",
        host="postgres-db",
        port=5432,
        user="airflow",
        password="airflow",
        database="learn_base",):

    with psycopg2.connect(host=host, port=port,
                          database=database, user=user,
                          password=password) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()

    print("DDL скрипт выполнен успешно")


