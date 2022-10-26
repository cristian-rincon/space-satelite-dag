from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}


def pass_value(**context):
    print(context["task_instance"].xcom_pull(task_ids="task_2"))

with DAG(
    dag_id="xcoms_dag",
    description="Xcoms Dag",
    schedule_interval="@daily",
    start_date=datetime(2022, 9, 24),
    end_date=datetime(2022, 10, 25),
    default_args=default_args,
) as dag:

    t1 = BashOperator(
        task_id="task_1",
        bash_command="sleep 5 && echo $((3*50))",

    )

    t2 = BashOperator(
        task_id="task_2",
        bash_command="sleep 3 && echo {{ ti.xcom_pull(task_ids='task_1') }}",
    )

    t3 = PythonOperator(
        task_id="task_3",
        python_callable=pass_value,
    )

    t1 >> t2 >> t3
    
