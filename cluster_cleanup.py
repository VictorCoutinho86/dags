from datetime import datetime, timedelta
import boto3
from airflow.decorators import dag, task
from kubernetes import client, config

@dag(
    "cleanup_k8s_aws_environment",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Limpeza de EBS e pods com erro",
    schedule="@hourly",  # roda a cada hora
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["cleanup", "aws", "k8s"],
)
def cluster_cleanup():
    @task
    def cleanup_ebs_volumes(**kwargs):
        ec2 = boto3.client("ec2")
        response = ec2.describe_volumes(
            Filters=[
                {"Name": "status", "Values": ["available"]}  # volumes "available" não estão anexados
            ]
        )

        cutoff_time = datetime.utcnow() - timedelta(minutes=15)

        for vol in response["Volumes"]:
            # Verifica se a data de criação é mais antiga que 15 min
            if "CreateTime" in vol and vol["CreateTime"].replace(tzinfo=None) < cutoff_time:
                vol_id = vol["VolumeId"]
                print(f"Deletando volume {vol_id}, não utilizado há mais de 15 minutos")
                try:
                    ec2.delete_volume(VolumeId=vol_id)
                except Exception as e:
                    print(f"Erro ao deletar volume {vol_id}: {e}")

    @task
    def cleanup_error_pods(**kwargs):
        # Carrega configuração do Kubernetes (assumindo que está rodando dentro do cluster)
        config.load_incluster_config()
        v1 = client.CoreV1Api()

        pods = v1.list_pod_for_all_namespaces(watch=False)
        for pod in pods.items:
            if pod.status.phase == "Failed" or pod.status.phase == "Error":
                name = pod.metadata.name
                namespace = pod.metadata.namespace
                print(f"Deletando pod {name} no namespace {namespace} com status {pod.status.phase}")
                try:
                    v1.delete_namespaced_pod(name=name, namespace=namespace)
                except Exception as e:
                    print(f"Erro ao deletar pod {name}: {e}")

    cleanup_error_pods() >> cleanup_ebs_volumes()
cluster_cleanup()
