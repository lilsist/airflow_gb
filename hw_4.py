import os
import datetime as dt
import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

# from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

file_name = 'titanic.csv'
url = 'https://web.stanford.edu/class/archive/cs/cs109.1166/stuff/titanic.csv'

default_args = {
    'owner': 'gb',
    'start_date': dt.datetime(2022, 7, 19),
    'retrice': 1,
    'retry_delay': dt.timedelta(minutes=1),
}


@dag(dag_id='test_flask', schedule_interval=None, default_args=default_args, start_date=days_ago(2), tags=['ae'])
def bild_shiwcase():
    first_task = BashOperator(
        task_id='greetings',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
    )

    @task
    def get_path(file: str):
        return os.path.join(os.path.expanduser('~'), file)

    @task
    def download_titanic_dataset(path: str):
        df = pd.read_csv(url)
        df.to_csv(path, encoding='utf-8')

    @task
    def pivot_dataset(path: str):
        titanic_df = pd.read_csv(path)

        df = titanic_df.pivot_table(index=['Sex'],
                                    columns=['Pclass'],
                                    values="Name",
                                    aggfunc='count').reset_index()

        df.to_csv(get_path('titanic_pivod.csv'))

    @task
    def mean_fare_per_class(path: str):
        titanic_df = pd.read_csv(path)

        df = titanic_df.pivot_table(
            index=['Pclass'],
            values='Fare',
            aggfunc='mean'
        ).reset_index()

        df.to_csv(get_path('titanic_mean_fares.csv'))

    loc_path = get_path(file_name)
    create_titanic_dataset = download_titanic_dataset(loc_path)
    pivot_titanic_dataset = pivot_dataset(loc_path)
    mean_fares_titanic_dataset = mean_fare_per_class(loc_path)

    last_task = BashOperator(
        task_id='last_task',
        bash_command=f'echo "Pipeline finished! Execution date is {dt.date.today()}"',
    )

    first_task >> create_titanic_dataset >> [pivot_titanic_dataset, mean_fares_titanic_dataset] >> last_task