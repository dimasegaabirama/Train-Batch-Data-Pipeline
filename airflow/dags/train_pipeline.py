import hashlib

from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import chain, dag, task_group
from airflow.sdk.definitions.deadline import (
    AsyncCallback,
    DeadlineAlert,
    DeadlineReference,
)
from airflow.utils.task_group import TaskGroup
from docker.types import Mount
from pendulum import duration, from_format
from typing_extensions import Dict

from src.core.config import PipelineManager, SparkManager, TableManager
from src.models.data_config import PipelineStage

PIPELINE_CONFIG = PipelineManager().get_config()
SPARK_CONFIG = SparkManager()
TABLE_CONFIG = TableManager()

POOL_NAME = "spark_job_limit_pool"
POOL_SLOTS: Dict[PipelineStage, int] = {
    "bronze": 1,
    "silver": 2,
    "gold": 2,
}

# Deterministic driver-port allocation, so that concurrent jobs never collide.
PORT_BASE = 20000
PORT_RANGE_SIZE = 1000

DEFAULT_ARGS = {
    "owner": "Dimas Ega Abirama | Data Engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": duration(minutes=3),
    "max_retry_delay": duration(minutes=40),
    "retry_exponential_backoff": True,
    "execution_timeout": duration(hours=1),
}


async def callback_function(**kwargs):
    """Fired when a DAG run misses its deadline (see DeadlineAlert below)."""
    dag_run = kwargs.get("dag_run")
    alert_type = kwargs.get("alert_type")
    severity = kwargs.get("severity")

    print(
        f"SEVERITY : {severity} | Dag {dag_run.dag_id} missed deadline | "
        f"DagRun: {dag_run}, Alert Type: {alert_type} !!"
    )


def dns_safe(name: str) -> str:
    """
    Make a string safe to use as a Docker/DNS hostname AND container name.

    Docker's embedded DNS resolves other containers on a user-defined
    network by their `container_name` (and any network alias) - NOT by
    the `hostname` set inside the container. Underscores are not valid
    in DNS hostnames, so if `container_name` and `hostname` ever diverge
    on this point, the driver becomes unreachable from other containers
    with an "Invalid Spark URL" error. Always sanitize BOTH with this
    same function so they stay identical.
    """
    return name.replace("_", "-")


def get_deterministic_port(
    stage: str,
    table_name: str,
    base_port: int = PORT_BASE,
    range_size: int = PORT_RANGE_SIZE,
) -> int:
    """Derive a stable, collision-resistant driver port from stage+table."""
    seed = f"{stage}_{table_name}"
    hash_digest = hashlib.md5(seed.encode()).hexdigest()
    offset = int(hash_digest, 16) % range_size

    return base_port + offset


@dag(
    dag_id="Train_Batch_Pipeline",
    # deadline=DeadlineAlert(
    #     reference=DeadlineReference.DAGRUN_QUEUED_AT,
    #     interval=duration(minutes=20),
    #     callback=AsyncCallback(
    #         callback_function,
    #         kwargs={"alert_type": "time_exceeded", "severity": "high"},
    #     ),
    # ),
    schedule=PIPELINE_CONFIG.schedule,
    start_date=from_format(
        PIPELINE_CONFIG.start_date, "YYYY-MM-DD", tz=PIPELINE_CONFIG.timezone
    ),
    max_consecutive_failed_dag_runs=3,
    fail_fast=True,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    dagrun_timeout=duration(hours=1),
    default_args=DEFAULT_ARGS,
    tags=["pipeline", "batch", "train"],
    description=(
        "Orchestrates the end-to-end train batch data pipeline, from data "
        "extraction and transformation to loading into the data warehouse."
    ),
)
def train_pipeline():

    def make_command(
        stage: PipelineStage, table_name: str, data_quality_enabled: bool = True
    ) -> list[str]:
        command = [
            "python3",
            "-m",
            "src.app.run_pipeline",
            "-stg", stage,
            "-tbl", table_name,
            "-cfg", "/app/config/pipeline-config.yaml",
            "-env", "/app/.env.global",
            "-start", "{{ data_interval_start.subtract(days=1).start_of('day') | ds }}",
            "-end", "{{ data_interval_end | ds }}",
        ]

        if data_quality_enabled:
            command.append("--data_quality")

        return command

    def make_mount() -> list[Mount]:
        return [
            Mount(
                source=PIPELINE_CONFIG.config_host_path,
                target="/app/config/pipeline-config.yaml",
                type="bind",
            ),
            Mount(
                source=PIPELINE_CONFIG.env_host_path,
                target="/app/.env.global",
                type="bind",
            ),
        ]

    def make_spark_job(
        stage: PipelineStage, table_name: str, data_quality_enabled: bool = True
    ) -> DockerOperator:
        spark_submit_image_name = SPARK_CONFIG.get_config().submit_image_name

        driver_name = dns_safe(f"spark-submit-{stage}-{table_name}")
        driver_port = get_deterministic_port(stage, table_name)

        return DockerOperator(
            task_id=f"run_{stage}_{table_name}",
            image=spark_submit_image_name,
            command=make_command(stage, table_name, data_quality_enabled),
            container_name=driver_name,
            hostname=driver_name,
            port_bindings={driver_port: driver_port},
            docker_url="tcp://docker-proxy:2375",
            network_mode="data_eng_net",
            mount_tmp_dir=False,
            mounts=make_mount(),
            auto_remove="force",
            environment={
                "SPARK_DRIVER_HOST": driver_name,
                "SPARK_DRIVER_PORT": str(driver_port),
                "CONFIG_PATH": "/app/config/pipeline-config.yaml",
                "ENV_PATH": "/app/.env.global"
            },
            pool=POOL_NAME,
            pool_slots=POOL_SLOTS[stage],
        )

    def make_empty_task(task_id: str) -> EmptyOperator:
        return EmptyOperator(task_id=task_id)

    def build_task(
        task_map: Dict[str, DockerOperator],
        stage: PipelineStage,
        table_name: str,
        data_quality_enabled: bool = True,
    ) -> DockerOperator:
        """Return the existing task for `table_name`, creating it on first use."""
        task = task_map.get(table_name)

        if task is None:
            task = make_spark_job(stage, table_name, data_quality_enabled)
            task_map[table_name] = task

        return task

    def make_task_group(stage: PipelineStage) -> TaskGroup:
        @task_group(group_id=f"{stage}_tasks")
        def task_group_stage(stage: PipelineStage) -> None:
            task_map: Dict[str, DockerOperator] = {}

            for table_name in TABLE_CONFIG.get_tablenames(stage):
                build_task(task_map, stage, table_name)

                table_deps = TABLE_CONFIG.get_table_deps(table_name, stage)
                if not table_deps:
                    continue

                if TABLE_CONFIG.is_seed_table(table_name):
                    continue

                for name, deps in table_deps.items():
                    current_task = build_task(task_map, stage, name)

                    if TABLE_CONFIG.is_seed_table(name):
                        continue

                    for dep in deps:

                        if(
                            dep.namespace != stage or
                            dep.catalog != PIPELINE_CONFIG.catalog_type or
                            dep.name not in TABLE_CONFIG.get_tablenames(stage) or 
                            TABLE_CONFIG.is_seed_table(dep.name)
                           ):
                            continue

                        dep_task = build_task(task_map, stage, dep.name)
                        chain(dep_task, current_task)

        return task_group_stage(stage)

    bronze = make_task_group("bronze")
    silver = make_task_group("silver")
    gold = make_task_group("gold")

    (
        make_empty_task("bronze")
        >> bronze
        >> make_empty_task("silver")
        >> silver
        >> make_empty_task("gold")
        >> gold
    )


train_pipeline()