import datetime as dt
from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="01_unscheduled",
    start = dt.datetime(year=2002,month=1,day=1),
    end = dt.datetime(year=2022, month=12, day=30),
    schedule_interval=None
)

fetch_events = BashOperator(
    task_id="fetch_users_data",
    bash_command=(
        "mkdir -p /data &&"
        "curl -o /data/events.json"
        "http://localhost:5100/events"
    ),
    dag=dag
)

def _calculate_stats(input_path, output_path):
    """Calculates events stats"""
    events = pd.read_json(input_path)
    stats = events.groupby(["date","user"]).size().reset_index()
    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)

fetch_events >> _calculate_stats
