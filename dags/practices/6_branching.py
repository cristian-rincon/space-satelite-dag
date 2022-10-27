from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}


def _choose(**context):
    if int(context["task_instance"].xcom_pull(task_ids="task_1")) > 150:
        return "task_3"
    return "task_4"


with DAG(
    dag_id="branching_dag",
    description="Branching Dag",
    schedule_interval="@daily",
    start_date=datetime(2022, 10, 24),
    end_date=datetime(2022, 10, 26),
    default_args=default_args,
) as dag:

    t1 = BashOperator(
        task_id="task_1",
        bash_command="sleep 5 && echo $(($RANDOM * 51))",
    )

    tbranch = BranchPythonOperator(
        task_id="task_branch",
        python_callable=_choose,
    )

    t3 = BashOperator(
        task_id="task_3",
        bash_command="echo 'El valor es mayor a 150'",
    )

    t4 = BashOperator(
        task_id="task_4",
        bash_command="echo 'El valor es menor a 150'",
    )

    t1 >> tbranch >> [t3, t4]
