from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    dag_id="sensor_dag",
    description="Sensor Dag",
    schedule_interval="@daily",
    start_date=datetime(2022, 9, 24),
    end_date=datetime(2022, 10, 25),
) as dag:

    t1 = ExternalTaskSensor(
        task_id="task_1",
        external_dag_id="sandbox_dag",
        external_task_id="task_4",
        allowed_states=["success"],
        poke_interval=5,
    )
