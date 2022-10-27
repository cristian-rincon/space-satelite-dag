from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def _generate_data(**kwargs):
    import pandas as pd

    data = pd.DataFrame(
        {
            "student": ["John Doe", "Jane Doe", "John Smith", "Jane Smith", "Sofia Smith"],
            "timestamp": [
                kwargs["logical_date"],
                kwargs["logical_date"],
                kwargs["logical_date"],
                kwargs["logical_date"],
                kwargs["logical_date"],
            ],
        }
    )
    data.to_csv(f"/tmp/data_{kwargs['ds_nodash']}.csv", header=True)


with DAG(
    dag_id="space_exploration_dag",
    description="Space Exploration DAG",
    schedule_interval="@daily",
    start_date=datetime(2022, 9, 24),
    end_date=datetime(2022, 10, 25),
) as dag:

    # Simulate NASA's confirmation of the launch

    launch_confirmation = BashOperator(
        task_id="launch_confirmation",
        bash_command="sleep 5 && echo 'The launch is confirmed!' > /tmp/nasa_response_{{ ds_no_dash }}.txt",
    )

    # Get data from NASA's API

    get_data = BashOperator(
        task_id="get_data",
        bash_command="curl -o /tmp/history.json 'https://api.spacexdata.com/v4/history'",
    )


    # Simulate NASA's confirmation of the landing

    landing_confirmation = PythonOperator(
        task_id="landing_confirmation",
        python_callable=_generate_data,
        provide_context=True,
    )

    # Send notification to the team

    send_notification = BashOperator(
        task_id="send_notification",

