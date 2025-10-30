from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import time

# Аргументы по умолчанию для DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Создаем DAG
with DAG(
        'hello_world',
        default_args=default_args,
        description='Мой первый Hello World DAG',
        schedule_interval=timedelta(days=1),  # Запускать каждый день
        catchup=False,  # Не запускать за прошлые периоды
        tags=['hello_world', 'training'],
) as dag:
    # Таска 1: Пустышка (начало)
    start_task = DummyOperator(
        task_id='start_task',
    )


    # Таска 2: Python функция с Hello World
    def print_hello_world():
        print("🎉 Hello World from Airflow!")
        time.sleep(30)
        print(f"Запущено в: {datetime.now()}")
        return "Hello World выполнено успешно!"


    hello_world_task = PythonOperator(
        task_id='hello_world_task',
        python_callable=print_hello_world,
    )

    # Таска 3: Пустышка (конец)
    end_task = DummyOperator(
        task_id='end_task',
    )

    # Определяем порядок выполнения тасок
    start_task >> hello_world_task >> end_task

    # Документация для DAG
    dag.doc_md = """
    ## Мой первый DAG

    Простой Hello World DAG для обучения.

    ### Задачи:
    1. **start_task** - Пустышка для начала
    2. **hello_world_task** - Печатает Hello World
    3. **end_task** - Пустышка для завершения
    """

    # Документация для тасок
    start_task.doc_md = "Начальная пустышка"
    hello_world_task.doc_md = "Печатает Hello World в лог"
    end_task.doc_md = "Конечная пустышка"