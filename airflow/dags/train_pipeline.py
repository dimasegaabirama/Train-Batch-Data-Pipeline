from airflow.sdk import task_group, chain, dag, task
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import AsyncCallback, DAG, DeadlineAlert, DeadlineReference
from docker.types import Mount
from datetime import datetime, timedelta


DEFAULT_ARGS = {
    'owner': 'Dimas Ega Abirama | Data Engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1)
}

async def callback_function(**kwargs):
    dag_run = kwargs.get('dag_run')
    alert_type = kwargs.get('alert_type')
    severity = kwargs.get('severity')

    print(f"🚨 SEVERITY : {severity} | Dag {dag_run.dag_id} missed deadline | DagRun: {dag_run}, Alert Type: {alert_type} !!")

@dag(
    dag_id="super_pipeline_komplit",
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(minutes=20),
        callback=AsyncCallback(
            callback_function,
            kwargs={"alert_type": "time_exceeded", "severity": "high"}
        )
    ),
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    fail_fast=True,
    max_consecutive_failed_runs=3,
    catchup=False,
    
    max_active_runs=1,
    max_active_tasks=5,

    dagrun_timeout=timedelta(hours=3),

    default_args=DEFAULT_ARGS,
    # params={
    #     "mode_testing": False,
    #     "ukuran_batch": 100
    # },
    
    tags=["pipeline", "batch", "train"],
    description="Pipeline utama untuk penarikan data train batch, transformasi, dan load ke data warehouse"
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