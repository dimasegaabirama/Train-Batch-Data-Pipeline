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
    dag_id="etl_pipeline",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["etl"],
    max_active_runs=1,
    max_active_tasks=3,
)
def train_pipeline():

    def make_command() -> list[str]:
        return [
            "python3", "-m", "main",
            "-cfg", Variable.get("config_path"),
            "-env", Variable.get("secret_env_target"),
            "-start", "{{ data_interval_start | ds }}",
            "-end",   "{{ data_interval_end   | ds }}",
        ]

    def make_mount() -> Mount:
        return Mount(
            source=Variable.get("secret_env_path"),
            target=Variable.get("secret_env_target"),
            type="bind"
        )

    def make_docker_task(stage: str) -> DockerOperator:

        return DockerOperator(
            task_id=f"run_{stage}",
            image=Variable.get("spark_submit_image_name"),
            command=make_command(),
            container_name=f"spark_submit_{stage}",
            docker_url="tcp://socat-docker:2375",
            network_mode="data_eng_net",
            mount_tmp_dir=False,
            mounts=[make_mount()],
            auto_remove="force"
        )

    # ── DAG Structure ──────────────────────────────────────────────
    stages = Variable.get("stages", deserialize_json=True)
    previous_group = None

    for stage in stages:
        current_group = make_docker_task(stage=stage)

        if previous_group:
            previous_group >> current_group

        previous_group = current_group


train_pipeline()