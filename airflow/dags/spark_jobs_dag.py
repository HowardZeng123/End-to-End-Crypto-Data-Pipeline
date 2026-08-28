from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_spark_job(job_file):
    """
    Kết nối vào container spark-master đang chạy và gọi lệnh spark-submit.
    Bằng cách này, ta không cần cài đặt Spark trong môi trường của Airflow.
    """
    import docker
    client = docker.from_env()
    
    # Tìm container spark-master
    try:
        container = client.containers.get('spark-master')
    except docker.errors.NotFound:
        raise Exception("Không tìm thấy container 'spark-master'. Hãy đảm bảo nó đang chạy.")
    
    packages = "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0"
    command = f'/opt/spark/bin/spark-submit --packages {packages} /opt/spark/jobs/{job_file}'
    logging.info(f"Đang thực thi lệnh: {command}")
    
    exit_code, output = container.exec_run(
        cmd=command,
        user='root',
        stream=False
    )
    
    log_output = output.decode('utf-8')
    logging.info(f"Kết quả thực thi:\n{log_output}")
    
    if exit_code != 0:
        raise Exception(f"Job {job_file} thất bại với exit code {exit_code}")
    else:
        logging.info(f"Job {job_file} chạy thành công!")

with DAG(
    'crypto_spark_jobs',
    default_args=default_args,
    description='Chạy các Spark Jobs cho dữ liệu crypto hàng ngày',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Task 1: Silver Job
    silver_task = PythonOperator(
        task_id='run_silver_job',
        python_callable=run_spark_job,
        op_kwargs={'job_file': 'silver_job.py'},
    )

    # Task 2: Batch Job
    batch_task = PythonOperator(
        task_id='run_batch_job',
        python_callable=run_spark_job,
        op_kwargs={'job_file': 'batch_job.py'},
    )

    # Đảm bảo silver job chạy xong rồi mới tới batch job
    silver_task >> batch_task
