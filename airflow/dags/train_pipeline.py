from airflow.sdk import task_group, chain, dag, task
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta


DEFAULT_ARGS = {
    "owner": "data_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="train_pipeline",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["etl"],
    max_active_runs=1,
    max_active_tasks=3,
)
def train_pipeline():

    def resolve_variable(var_name: str, default_value: str = None) -> str:
        var_value = {
            "spark_submit_image_name": Variable.get("spark_submit_image_name"),
            "secret_env_path": Variable.get("secret_env_path"),
            "secret_env_target": Variable.get("secret_env_target"),
            "config_path": Variable.get("config_path"),
            "data_quality_enabled": Variable.get("data_quality_enabled")
        }
        return var_value.get(var_name, default_value)

    def make_command(stage: str) -> list[str]:
        return [
            "python3", "-m", "main",
            "-stg", stage,
            "-cfg", resolve_variable("config_path"),
            "-env", resolve_variable("secret_env_target"),
            "-start", "{{ data_interval_start | ds }}",
            "-end",   "{{ data_interval_end   | ds }}",
            "--data_quality" if resolve_variable("data_quality_enabled") == "true" else ""
        ]

    def make_mount() -> Mount:
        return Mount(
            source=resolve_variable("secret_env_path"),
            target=resolve_variable("secret_env_target"),
            type="bind"
        )

    def make_spark_job(stage: str) -> DockerOperator:
        return DockerOperator(
            task_id=f"run_{stage}",
            image=resolve_variable("spark_submit_image_name"),
            command=make_command(stage),
            container_name=f"spark_submit_{stage}",
            docker_url="tcp://socat-docker:2375",
            network_mode="data_eng_net",
            mount_tmp_dir=False,
            mounts=[make_mount()],
            auto_remove="force"
        )

    stages = ["bronze", "silver", "gold"]
    previous_group = None

    for stage in stages:
        current_group = make_spark_job(stage=stage)

        if previous_group:
            previous_group >> current_group

        previous_group = current_group


train_pipeline()