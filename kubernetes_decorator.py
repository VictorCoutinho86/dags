from datetime import datetime
from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="example_kubernetes_decorator",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    tags=["example", "cncf", "kubernetes"],
    catchup=False,
) as dag:
    @task.kubernetes(
        image="python:3.9-slim-buster",
        name="k8s_test",
        namespace="airflow",
        in_cluster=True
    )
    def execute_in_k8s_pod():
        import time

        print("Hello from k8s pod")
        time.sleep(2)

    @task.kubernetes(image="python:3.9-slim-buster", namespace="airflow", in_cluster=True)
    def print_pattern():
        n = 5
        for i in range(n):
            # inner loop to handle number of columns
            # values changing acc. to outer loop
            for _ in range(i + 1):
                # printing stars
                print("* ", end="")

            # ending line after each row
            print("\r")

    execute_in_k8s_pod_instance = execute_in_k8s_pod()
    print_pattern_instance = print_pattern()

    execute_in_k8s_pod_instance >> print_pattern_instance
