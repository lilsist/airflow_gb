import datetime as dt
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'gb',
    'start_date': dt.datetime(2022,7, 19),
    'retrice': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

with DAG(
    dag_id='titanic_hive',
    schedule_interval=None,
    default_args=default_args,
) as dag:

    create_titanic_dataset = BashOperator(
        task_id='download_titanic_dataset',
        bash_command='''TITANIC_FILE="titanic-{{ execution_date.int_timestamp }}.csv" && \
            wget https://web.stanford.edu/class/archive/cs/cs109.1166/stuff/titanic.csv -O ${TITANIC_FILE} &&\
            hdfs dfs -mkdir -p /datasets/ %% \
            hdfs dfs -put $TITANIC_FILE /datasets/ %% \
            rm $TITANIC_FILE && \
            echo "/datasets/$TITANIC_FILE"
            ''',
    )

    with TaskGroup('prepare_table') as prepare_table:

        drop_hive_table = HiveOperator(
            task_id='drop_hive_table',
            hql='DROP TABLE IF EXISTS titanic;',
            # hive_cli_conn_id = "hive_local",
        )

        create_hive_table = HiveOperator(
            task_id='create_hive_table',
            hql='''CREATE TABLE IF NOT EXISTS titanic (
                Survived INT,
                Pclass INT, 
                Name STRING,
                Sex STRING, 
                Age INT, 
                SibSp INT,
                ParChild INT,
                Fare DOUBLE
                )
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY ',' 
                STORED AS TEXTFILE
                TBLPROPERTIES('skip.header.line.count'='1');
                ''',
            # hive_cli_conn_id = "hive_local",
        )

        drop_hive_table >> create_hive_table

    load_titanic_dataset = HiveOperator(
        task_id='load_data_to_hive',
        hql='''LOAD DATA INPATH '{{ task_instance.xcom_pull(task_ids='download_titanic_dataset', key='return_value') }}' 
        INTO TABLE titanic;''',
        # hive_cli_conn_id = "hive_local",
    )

    show_avg_fare = BashOperator(
        task_id='show_avg_fare',
        bash_command='''beeline -u jdbc:hive://localhost:10000 \
        -e "SELECT Pclass, avg(Fare) FROM titanic GROUP BY Pclass;" | tr "n" ";" ''',
    )

    def format_message(**kwargs):
        flat_message = kwargs['ti'].xcom_pull(task_ids='show_avg_fare', key='return_value')
        message = flat_message.replace(';', '\n')

        print(message)

        kwargs['ti'].xcom_push(key='telegram_message', value=message)

    prepare_message = PythonOperator(
        task_id='prepare_message',
        python_callable=format_message,
    )

    send_result = TelegramOperator(
        task_id='send_success_message_telegram',
        telegram_conn_id='telegram_conn_id',
        chat_id='881275237',
        text='''Rireline {{ execution_date.int_timestamp }} is done. Result: 
        {{ ti.xcom_pull(task_ids='prepare_message', key='telegram_message')}} ''',
    )

    create_titanic_dataset >> prepare_table >> load_titanic_dataset >> show_avg_fare >> prepare_message >> send_result