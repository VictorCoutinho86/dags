"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from datetime import datetime, timedelta
from airflow.models import DAG, Connection
from airflow.operators.bash import BashOperator
from airflow.providers.slack.notifications.slack import send_slack_notification


# DAG arguments
failure_message = Connection.get_connection_from_secrets("messages").extra_dejson.get("on_failure_message")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 10),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_slack_notification(
        text=failure_message,
        channel="bi-airflow-victor",
        username="airflow",
    ),
}

# Instantiating DAG, its arguments and schedule interval
with DAG(dag_id="new_tutorial_v1.0.0", default_args=default_args, schedule_interval=timedelta(1), catchup=False) as dag:
    
    bash_command = BashOperator(
        task_id="new_command",
        bash_command="echo \"It's time to run Airflow commands\""
    )
    
    airflow_command = BashOperator(
        task_id="airflow_command",
        bash_command="airflow connections get messages -o json"
    )
    
    bash_command >> airflow_command
