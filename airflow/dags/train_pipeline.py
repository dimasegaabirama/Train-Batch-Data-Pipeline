from collections import deque
from pathlib import Path

from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import chain, dag, task_group
from airflow.sdk.definitions.deadline import (
    AsyncCallback,
    DeadlineAlert,
    DeadlineReference,
)
from docker.types import Mount
from pendulum import duration, from_format
from typing_extensions import Dict

from src.core.config import PipelineManager, SparkManager, TableManager
from src.models.data_config import StageType

PIPELINE_CONFIG = PipelineManager().get_config()
SPARK_CONFIG = SparkManager()
TABLE_CONFIG = TableManager()

TIMEZONE = "Asia/Jakarta"

PATH_TO_ENV = Path(__file__).parents[3] / ".env.global"
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
    dag_run = kwargs.get("dag_run")
    alert_type = kwargs.get("alert_type")
    severity = kwargs.get("severity")

    print(
        f"🚨 SEVERITY : {severity} | Dag {dag_run.dag_id} missed deadline | DagRun: {dag_run}, Alert Type: {alert_type} !!"
    )


@dag(
    dag_id="Train_Batch_Pipeline",
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=duration(minutes=20),
        callback=AsyncCallback(
            callback_function,
            kwargs={"alert_type": "time_exceeded", "severity": "high"},
        ),
    ),
    schedule=PIPELINE_CONFIG.schedule,
    start_date=from_format(PIPELINE_CONFIG.start_date, "YYYY-MM-DD", tz=TIMEZONE),
    max_consecutive_failed_dag_runs=3,
    fail_fast=True,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    dagrun_timeout=duration(hours=3),
    default_args=DEFAULT_ARGS,
    tags=["pipeline", "batch", "train"],
    description="Pipeline utama untuk penarikan data train batch, transformasi, dan load ke data warehouse",
)
def train_pipeline():

    def make_command(
        stage: StageType, table_name: str, data_quality_enabled: bool = True
    ) -> list[str]:
        return [
            "python3",
            "-m",
            "src.app.run_pipeline",
            "-stg",
            stage,
            "-tbl",
            f"{table_name}_{stage}",
            "-cfg",
            PIPELINE_CONFIG.config_path,
            "-env",
            PIPELINE_CONFIG.env_path,
            "-start",
            "{{ data_interval_start }}",
            "-end",
            "{{ data_interval_end }}",
            "--data_quality" if data_quality_enabled else "",
        ]

    def make_mount() -> Mount:
        return Mount(
            source=str(PATH_TO_ENV.absolute()),
            target=PIPELINE_CONFIG.env_path,
            type="bind",
        )

    def make_spark_job(
        stage: StageType, table_name: str, data_quality_enabled: bool = True
    ) -> DockerOperator:

        spark_submit_image_name = SPARK_CONFIG.get_config().submit_image_name
        driver_host = SPARK_CONFIG.get_stage_config(stage).config.get(
            "spark.driver.host"
        )
        driver_port = SPARK_CONFIG.get_stage_config(stage).config.get(
            "spark.driver.port"
        )

        return DockerOperator(
            task_id=f"run_{stage}_{table_name}",
            image=spark_submit_image_name,
            command=make_command(stage, table_name, data_quality_enabled),
            container_name=f"spark_submit_{stage}_{table_name}",
            hostname=driver_host,
            docker_url="tcp://docker-proxy:2375",
            port_bindings={driver_port: driver_port},
            network_mode="data_eng_net",
            mount_tmp_dir=False,
            mounts=[make_mount()],
            auto_remove="force"
        )

    def make_empty_task(task_id: str) -> EmptyOperator:
        return EmptyOperator(task_id=task_id)

    def build_task(
        task_map: Dict[str, DockerOperator],
        stage: StageType,
        table_name: str,
        data_quality_enabled: bool = True,
    ):
        task = task_map.get(table_name)

        if not task:
            task = make_spark_job(
                stage, table_name, data_quality_enabled
            )
            task_map[table_name] = task

        return task


    def make_task_group(stage: StageType) -> task_group:

        @task_group(group_id=f"{stage}_tasks")
        def task_group_stage(stage: str):

            task_map = {}

            for table_name in TABLE_CONFIG.get_tablenames(stage):
                build_task(task_map, stage, table_name)

            
            for table_name in TABLE_CONFIG.get_tablenames(stage):

                current_task = build_task(
                    task_map,
                    stage,
                    table_name
                )

                table_deps = TABLE_CONFIG.get_table_deps(
                    table_name,
                    stage
                )

                if not table_deps:
                    continue

                for name, deps in table_deps.items():

                    current_task = build_task(
                        task_map,
                        stage,
                        name
                    )

                    for dep in deps:

                        if dep.namespace != stage:
                            continue

                        dep_task = build_task(
                            task_map,
                            stage,
                            dep.name
                        )

                        chain(
                            dep_task,
                            current_task
                        )
                        

        return task_group_stage(stage)


    bronze = make_task_group("bronze")
    silver = make_task_group("silver")
    gold = make_task_group("gold")

    make_empty_task("bronze") >> bronze >> make_empty_task("silver") >> silver >> make_empty_task("gold") >> gold

train_pipeline()
