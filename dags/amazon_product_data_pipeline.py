from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
from jobs import file_downloader as fd
from utils.logging import LOG_LEVEL, LOG_FILE
from utils.airflow_connection import get_postgres_connection_credentials

###############################################
# Parameters
###############################################
import_items = {
    'metadata': {'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Movies_and_TV.json.gz'},
    'ratings': {'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/ratings_Movies_and_TV.csv'}
}

run_key = "{{run_id.split('T')[0]}}"
data_folder = '/usr/local/spark/resources/data'
app_folder = '/usr/local/spark/app'
pg_creds = get_postgres_connection_credentials('postgres_for_spark')
postgres_db = pg_creds['postgres_uri']
postgres_user = pg_creds['postgres_user']
postgres_pwd = pg_creds['postgres_password']
postgres_driver_jar = "/usr/local/spark/resources/jars/postgresql-9.4.1207.jar"
###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}
env_vars = {
    "LOG_FILE": LOG_FILE,
    "LOG_LEVEL": LOG_LEVEL
}

dag = DAG(
    dag_id="amazon-product-data-pipeline",
    description="This DAG runs a python app to download files in parallel manner, parse them, transform the data and aggregate it",
    default_args=default_args,
    schedule_interval=timedelta(1)
)


task_def = {}
for k, v in import_items.items():
    for url in v:
        obj_name = url.split('/')[-1].split('.')[0].lower()
        task_def[url] = {
            'obj_type': k,
            'obj_name': obj_name,
            'target_folder': f'{data_folder}/{run_key}/{k}/{obj_name}'
        }


def create_subdag(parent_dag, subdag_name, default_args):
    return DAG("{}.{}".format(parent_dag.dag_id, subdag_name),
               schedule_interval=parent_dag.schedule_interval,
               start_date=parent_dag.start_date,
               default_args=default_args)


def create_download_files_operator():
    download_sub_dag = create_subdag(dag, 'download_files', default_args)

    prev_task = None
    for k, v in task_def.items():

        download_file_task = PythonOperator(
            task_id=f'download_{v["obj_type"]}_{v["obj_name"]}',
            python_callable=fd.parallel_download,
            op_kwargs={
                'url': k,
                'target_folder': v['target_folder']
            },
            dag=download_sub_dag)

        if prev_task:
            prev_task >> download_file_task
        prev_task = download_file_task

    return SubDagOperator(
        subdag=download_sub_dag,
        task_id='download_files',
        dag=dag
    )


download_files = create_download_files_operator()


def create_process_files_operator():
    download_sub_dag = create_subdag(dag, 'process_files', default_args)

    prev_task = None
    for k, v in task_def.items():
        task_id = f'spark_process_{v["obj_type"]}_{v["obj_name"]}'
        spark_extract_task = SparkSubmitOperator(
            task_id=task_id,
            application=f'{app_folder}/raw_data_processor.py',
            name=task_id,
            conn_id='spark_default',
            application_args=[v['obj_name'], v['target_folder'],
                              v['obj_type'], postgres_db, postgres_user, postgres_pwd],
            jars=postgres_driver_jar,
            driver_class_path=postgres_driver_jar,
            env_vars=env_vars,
            dag=download_sub_dag)

        if prev_task:
            prev_task >> spark_extract_task
        prev_task = spark_extract_task

    return SubDagOperator(
        subdag=download_sub_dag,
        task_id='process_files',
        dag=dag
    )


process_files = create_process_files_operator()

task_id = f'spark_process_month_dm'
spark_dm_task = SparkSubmitOperator(
    task_id=task_id,
    application=f'{app_folder}/month_dm_processor.py',
    name=task_id,
    conn_id='spark_default',
    application_args=['meta_movies_and_tv', 'ratings_movies_and_tv',
                      'dm_ratings_by_month', postgres_db, postgres_user, postgres_pwd],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    env_vars=env_vars,
    dag=dag)

download_files >> process_files >> spark_dm_task
