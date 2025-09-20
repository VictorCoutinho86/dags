from datetime import datetime, timedelta
import requests
import boto3
import pandas as pd
from airflow.decorators import dag, task


@dag(
    dag_id="spot_instance_analysis",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["aws", "spot", "ec2"],
)
def spot_instance_analysis_dag():

    @task
    def get_spot_interruption_data():
        url = "https://spot-bid-advisor.s3.amazonaws.com/spot-advisor-data.json"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Erro ao buscar dados do Spot Advisor: {e}")
            return None

    @task
    def filter_instances(data, region="us-east-1"):
        if not data:
            return []

        instances = {
            name: dict(name=name, spot_price=0, **resources)
            for name, resources in data["instance_types"].items()
        }

        labels = {label["index"]: label["label"] for label in data["ranges"]}

        for region_name, region_data in data["spot_advisor"].items():
            if region_name == region:
                for instance_name, metadata in region_data["Linux"].items():
                    instances[instance_name]["interruption_frequency"] = labels.get(metadata["r"], "N/A")

        instance_list = [
            meta["name"]
            for meta in instances.values()
            if meta.get("interruption_frequency") == "<5%" and meta["cores"] >= 2 and meta["ram_gb"] >= 2
        ]

        return {"instances": instances, "instance_list": instance_list}

    @task
    def get_spot_prices(filtered_data, region="us-east-1"):
        if not filtered_data or not filtered_data["instance_list"]:
            return []

        client = boto3.client("ec2", region_name=region)
        yesterday = datetime.now() - timedelta(seconds=1)

        response = client.describe_spot_price_history(
            StartTime=yesterday,
            ProductDescriptions=["Linux/UNIX"],
            InstanceTypes=filtered_data["instance_list"],
        )

        final_list = {}
        for metadata in response["SpotPriceHistory"]:
            itype = metadata["InstanceType"]
            if itype not in final_list:
                final_list[itype] = {
                    "instance": itype,
                    "spot_price": float(metadata["SpotPrice"]),
                    "cpu": filtered_data["instances"][itype]["cores"],
                    "memory": filtered_data["instances"][itype]["ram_gb"],
                    "memory_per_cpu": filtered_data["instances"][itype]["ram_gb"] / filtered_data["instances"][itype]["cores"]
                    if filtered_data["instances"][itype]["cores"] > 0 else 0,
                    "interruption_frequency": filtered_data["instances"][itype]["interruption_frequency"]
                }
            else:
                final_list[itype]["spot_price"] = max(
                    final_list[itype]["spot_price"], float(metadata["SpotPrice"])
                )

        return sorted(final_list.values(), key=lambda x: x["spot_price"])

    @task
    def print_results(final_list):
        if final_list:
            df = pd.DataFrame(final_list)
            print(df.to_markdown())
        else:
            print("Nenhuma instância encontrada.")

    # Fluxo da DAG
    data = get_spot_interruption_data()
    filtered_data = filter_instances(data)
    final_list = get_spot_prices(filtered_data)
    print_results(final_list)


dag_instance = spot_instance_analysis_dag()
