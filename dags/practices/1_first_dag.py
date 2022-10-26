from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from custom_operator import HelloOperator

from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime, timedelta

with DAG(
    dag_id="sandbox_dag",
    description="SandBox DAG",
    schedule_interval="@daily",
    start_date=datetime(2022, 9, 24),
    end_date=datetime(2022, 10, 25),
) as dag:

    # Empty Operator
    t1 = EmptyOperator(task_id="task_1")

    # BashOperator
    t2 = BashOperator(
        task_id="task_2",
        bash_command="echo 'Hello Airflow with BashOperator!'",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    
    # PythonOperator

    def print_hello():
        return "Hello Airflow with PythonOperator!"

    t3 = PythonOperator(
        task_id="task_3",
        python_callable=print_hello,
        retries=3,
        retry_delay=timedelta(seconds=5),
        depends_on_past=False,
    )

    # Custom Operator

    t4 = HelloOperator(
        task_id="task_4",
        name="Cristian",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Set task dependencies

    t1 >> [t2, t3] >> t4