from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    dag_id="airbnb_scraping",
    start_date=datetime(2026, 1, 1),
    schedule_interval='@weekly',
    description='dl les CSV from open data of airbnb , clean , and run KPI ',
    catchup=False,
) as dag:
    scraping = BashOperator(
        task_id="run_scraping",
        bash_command="/home/champi-n/perso/airbnb/run_scraping.sh",
    )
    clean_and_parquet_CSV = PythonOperator(
        task_id="clean_and_parquet_CSV",
        bash_command="/home/champi-n/perso/airbnb/run_scraping.sh",
    )
    compute = BashOperator(
        task_id="compute",
        bash_command="/home/champi-n/perso/airbnb/run_scraping.sh",
    )
    alert_task = PythonOperator(
    task_id='send_pipeline_alert',
    python_callable=send_error_metrics,
    trigger_rule=TriggerRule.ONE_FAILED # <--- Crucial : se lance si n'importe quelle tâche échoue
)
    [scraping ,clean_and_parquet_CSV ,compute] >> alert_task

