from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator


def _generate_data(**kwargs):
    import pandas as pd

    sample_data = {
            "student": ["John Doe", "Jane Doe", "John Smith", "Jane Smith", "Sofia Smith"],
            "timestamp": [
                kwargs["logical_date"],
                kwargs["logical_date"],
                kwargs["logical_date"],
                kwargs["logical_date"],
                kwargs["logical_date"],
            ],
        }

    data = pd.DataFrame(sample_data)
    data.to_csv(f"/tmp/data_{kwargs['ds_nodash']}.csv", header=True)
    return data.to_json()



DEFAULT_ARGS = {
    'email': ['cristian.o.rincon.b@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
}

with DAG(
    dag_id="space_exploration_dag",
    description="Space Exploration DAG",
    schedule_interval="@once",
    start_date=datetime(2022, 10, 24),
    end_date=datetime(2022, 10, 26),
    default_args=DEFAULT_ARGS,
) as dag:

    # Simulate NASA's confirmation of the launch

    launch_confirmation = BashOperator(
        task_id="launch_confirmation",
        bash_command="sleep 5 && echo 'The launch is confirmed!' > /tmp/nasa_response_{{ ds }}.txt",
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

    # Print the data from landing confirmation

    print_data = BashOperator(
        task_id="print_data",
        bash_command="echo {{ task_instance.xcom_pull(task_ids='landing_confirmation') }}",
    )

    # Send notification to the team

    send_notification = EmailOperator(
        task_id="send_notification",
        to="suannoca@gmail.com",
        subject="Space Exploration",
        html_content="""
            <h3>Space Exploration</h3>
            <p>Today we have new data from the space exploration!</p>
            
            <p>Regards,<br>
        """,
    )

    