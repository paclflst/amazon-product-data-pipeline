import sys
import pkgutil
print("package"*4)

print([name for _, name, _ in pkgutil.iter_modules(['/usr/local/airflow/dags'])] )
import site; site.getsitepackages()
print(site.getsitepackages())


from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from jobs import file_downloader as fd

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
spark_app_name = "Spark Hello World"
file_path = "/usr/local/spark/resources/data/airflow.cfg"

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

start = DummyOperator(task_id="start", dag=dag)

spark_job = PythonOperator(
    task_id="download_file",
    python_callable=fd.parallel_download,
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

start >> spark_job >> end