import sys
import pkgutil
print("package"*4)

print([name for _, name, _ in pkgutil.iter_modules(['/usr/local/airflow/dags'])] )
import site; site.getsitepackages()
print(site.getsitepackages())


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
from jobs import file_downloader as fd

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
postgres_db = "jdbc:postgresql://postgres/test"
postgres_user = "test"
postgres_pwd = "postgres"
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


dag = DAG(
        dag_id="file-download", 
        description="This DAG runs a python app to download files in parallel manner.",
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

prev_task = None
for k,v in task_def.items():

    download_file_task = PythonOperator(
        task_id=f'download_{v["obj_type"]}_{v["obj_name"]}',
        python_callable=fd.parallel_download,
        op_kwargs={
            'url': k,
            'target_folder': v['target_folder']
        },
        dag=dag)

    if prev_task:
        prev_task>>download_file_task
    prev_task = download_file_task

for k,v in task_def.items():
    task_id = f'spark_extract_{v["obj_type"]}_{v["obj_name"]}'
    spark_extract_job = SparkSubmitOperator(
        task_id=task_id,
        application=f'{app_folder}/raw_data_processor.py', # Spark application path created in airflow and spark cluster
        name=task_id,
        conn_id='spark_default',
        verbose=1,
        application_args=[v['obj_name'], v['target_folder'], v['obj_type'], postgres_db,postgres_user,postgres_pwd],
        jars=postgres_driver_jar,
        driver_class_path=postgres_driver_jar,
        dag=dag)

    prev_task>>spark_extract_job
    prev_task = spark_extract_job