from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
from scripts.transform_script import transform  # Импортируем функцию transform

# Пути к файлам
DATA_PATH = os.path.join(os.path.dirname(__file__), '../data/profit_table.csv')
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), '../data/flags_activity.csv')

# Функция для извлечения данных
def extract():
    df = pd.read_csv(DATA_PATH)
    return df

# Функция для загрузки данных
def load(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_task')
    transformed_data.to_csv(OUTPUT_PATH, mode='a', header=not os.path.exists(OUTPUT_PATH), index=False)

# Настройка DAG
default_args = {
    'owner': 'Baygozin Pavel',  # Укажите ваше имя
    'start_date': datetime(2024, 1, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Baygozin_pavel_dag',  # Название DAG
    default_args=default_args,
    description='DAG для расчёта витрины активности клиентов',
    schedule_interval='0 0 5 * *',  # Запуск каждый месяц 5-го числа
    catchup=False,
)

# Задачи
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load,
    provide_context=True,
    dag=dag,
)

# Определение порядка выполнения задач
extract_task >> transform_task >> load_task