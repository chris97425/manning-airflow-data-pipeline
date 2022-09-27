import datetime as dt
from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="06_templated_query",
    start_date = dt.datetime(year=2002,month=1,day=1),
    end_date = dt.datetime(year=2022, month=12, day=30),
    schedule_interval=dt.timedelta(minutes=3)
)

fetch_events = BashOperator(
    task_id="fetch_users_data",
    bash_command=(
        "mkdir -p /data && \
        curl -o /data/events.json \
        http://localhost:5100/events?\
            start_date={{execution_date.strftime('%Y-%m-%d')}}&\
            end_date={{next_execution_date.strftime('%Y-%m-')}}"
    ),
    dag=dag
)

def _calculate_stats(input_path, output_path):
    """Calculates events stats"""
    events = pd.read_json(input_path)
    stats = events.groupby(["date","user"]).size().reset_index()
    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)


get_stats = PythonOperator(
    task_id="get_stats",
    python_callable=_calculate_stats,
    op_kwargs={
        "input_path":"/tmp/events.json",
        "output_path":"/tmp/stats.csv"
    },
    dag=dag
)

fetch_events >> get_stats
