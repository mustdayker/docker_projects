from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

from tasks.external_task import time_task, all_upper

default_args = {
    'owner': 'mustdayker',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='decorators_test',
    default_args=default_args,
    description='Пример DAG с декораторами',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'decorators']
)
def my_example_dag():

    @task
    def start_task():
        """Задача начала выполнения DAG"""

        print("---"*10, "Начинаем DAG", "---"*10)
        print("Тут мы ничего не передаем")
        print("----"*20)
        return "started!!!"

    @task
    def continue_task(start_data):
        print("---"*10, "Продолжаем DAG", "---"*10)
        print("Тут мы передаем данные из таски start_task")
        print("Данные из START:", start_data)
        print("----" * 20)
        return "continued!!!"

    # @task
    # def now_time():
    #     print("----" * 20)
    #     print("----" * 20)
    #     return time_task()

    # ✅ Для готовых функций используем PythonOperator
    time_result = PythonOperator(
        task_id='time_result',
        python_callable=time_task  # ← напрямую
    )


    end = PythonOperator(
        task_id='end',
        python_callable=lambda **context: all_upper(context['ti'].xcom_pull(task_ids='time_result'))
    )


    # @task
    # def ending_task(time_data):
    #     print("---"*10, "Заканчиваем DAG", "---"*10)
    #     print("Тут мы передаем данные из таски now_time")
    #     print(all_upper(time_data))
    #     print("----" * 20)
    #     return 300


    # Определение порядка выполнения задач
    start = start_task()
    cont = continue_task(start)
    # time_result = now_time()
    # end = ending_task(time_result.output)


    (start >> cont >> time_result >> end)

# Создание DAG
my_example_dag()