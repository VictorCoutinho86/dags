"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow.models import DAG, Connection
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.providers.slack.notifications.slack import send_slack_notification


# DAG arguments
failure_message = failure_message = Connection.get_connection_from_secrets("on_failure_message").extra_dejson.get("message")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 1),
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
dag = DAG(dag_id="tutorial_v1.1.1", default_args=default_args, schedule_interval=timedelta(1), catchup=False)


# DAGs are composed of tasks. Each task perform a specific operation. 
# Tasks are executed by an Operator object 
# t1, t2 and t3 are examples of tasks created by instantiating operators

# BashOperator is a operator that performs a bash command

# t1 is a simple task that shows the date
t1 = BashOperator(task_id="print_date", bash_command=f"{{{{ conn.on_failure_message.description }}}}", dag=dag)

# t2 sleeps the execution for 5 seconds
t2 = BashOperator(task_id="sleep", bash_command="sleep 5", dag=dag)


# This is a templated command using Jinja templating
templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}" 
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

# t3 executes a bash command according to the jinja Template above 
# and also gets a parameter 'my_param' to be printed
t3 = BashOperator(
    task_id="templated",
    bash_command=templated_command,
    params={"my_param": "Parameter I passed in"},
    dag=dag,
)

# Here the dependencies between tasks are defined
# t2 will be executed after t1, and t3 will be executed after t2
# the lines below can be replaced by a single line:
# t1 >> t2 >> t3

t2.set_upstream(t1)
t3.set_upstream(t1)
