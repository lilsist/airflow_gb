import os
import datetime as dt
import pandas as pd
import requests
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

args = {
    'owner': 'airflow',  # Информация о владельце DAG
    'start_date': dt.datetime(2020, 12, 23),  # Время начала выполнения пайплайна
    'retries': 1,  # Количество повторений в случае неудач
    'retry_delay': dt.timedelta(minutes=1),  # Пауза между повторами
    'depends_on_past': False,  # Запуск DAG зависит ли от успешности окончания предыдущего запуска по расписанию
}


def get_path(file_name):
    print('OK')
    return os.path.join(os.path.expanduser('~'), file_name)


def download_titanic_dataset():
    print('Download started')
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    df.to_csv(get_path('titanic.csv'), encoding='utf-8')
    print('Download done')

 def pivot_dataset():
     titanic_df = pd.read_csv(get_path('titanic.csv'))

     df = titanic_df.pivot_table(index=['Sex'],
                                 columns=['Pclass'],
                                 values="Name",
                                 aggfunc='count').reset_index()

     df.to_csv(get_path('titanic_pivod.csv'))

 def mean_fare_per_class():
     titanic_df = pd.read_csv(get_path('titanic.csv'))

     df = titanic_df.pivot_table(
         index=['Pclass'],
         values='Fare',
         aggfunc='mean'
     ).reset_index()

     df.to_csv(get_path('titanic_mean_fares.csv'))

 with DAG(
     dag_id='titanic_pivot',
     schedule_interval=None,
     default_args=default_args,
 ) as dag:

     first_task = BashOperator(
         task_id='greetings',
         bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
     )

     create_titanic_dataset = PythonOperator(
         task_id='download_titanic_dataset',
         python_callable=download_titanic_dataset,
     )

     pivot_titanic_dataset = PythonOperator(
         task_id='pivot_dataset',
         python_callable=pivot_dataset,
     )

     mean_fares_titanic_dataset = PythonOperator(
         task_id='mean_fares_titanic_dataset',
         python_callable=mean_fare_per_class,
     )

     last_task = BashOperator(
         task_id='last_task',
         bash_command=f'echo "Pipeline finished! Execution date is { dt.date.today() }"',
     )

     first_task >> create_titanic_dataset >> [pivot_titanic_dataset, mean_fares_titanic_dataset] >> last_task