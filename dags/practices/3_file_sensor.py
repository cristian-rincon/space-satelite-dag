from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.sensors.filesystem import FileSensor

with DAG(
    dag_id="file_sensor_dag",
    description="File Sensor Dag",
    schedule_interval="@daily",
    start_date=datetime(2022, 9, 24),
    end_date=datetime(2022, 10, 25),
) as dag:

    t1 = BashOperator(
        task_id="task_1",
        bash_command="echo 'Hello Airflow with BashOperator!' >> /tmp/file.txt",
    )

    t2 = FileSensor(
        task_id="task_2",
        filepath="/tmp/file.txt",
        poke_interval=5,
    )

    t3 = BashOperator(
        task_id="task_3",
        bash_command="echo 'El archivo existe!'",
    )

    t1 >> t2 >> t3