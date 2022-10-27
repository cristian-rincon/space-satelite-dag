from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

templated_command = """
    {% for filename in params.filenames %}
        echo "Filename: {{ filename }}"
        echo "{{ ds }}"
    {% endfor %}
"""

with DAG(
    dag_id="template_dag",
    description="Template Dag",
    schedule_interval="@daily",
    start_date=datetime(2022, 9, 24),
    end_date=datetime(2022, 10, 25),
) as dag:

    t1 = BashOperator(
        task_id="task_1",
        bash_command=templated_command,
        params={"filenames": ["file1.txt", "file2.txt", "file3.txt"]},
        depends_on_past=True,
    )
