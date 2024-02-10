from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from DataFetchClean import etl_process,build_latest


dag_args = {
    'owner': 'sad7_5407',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        dag_id='OpenAQ',
        default_args=dag_args,
        schedule_interval='@daily',
        start_date=datetime(2024, 2, 10),
        catchup=False
) as dag:
    dfs_start = BashOperator(
        task_id='start_dfs',
        bash_command="start-dfs.sh"
    )

    extract_transform_load = PythonOperator(
        task_id='extract_transform_load',
        python_callable=etl_process()
    )

    delete_local_data = BashOperator(
        task_id='delete_local_data',
        bash_command='rm -r /home/sad7_5407/Desktop/Data\ Engineering/data/*'
    )
    build_latest_table = PythonOperator(
        task_id='build_latest_table',
        python_callable=build_latest()
    )

    dfs_stop = BashOperator(
        task_id='start_dfs',
        bash_command="stop-dfs.sh"
    )