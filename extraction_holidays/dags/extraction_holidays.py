from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from sness.datalake.metadata import Environment
from sness.pipeline.custom_operators.cloud_storage_to_big_query_operator import (
    CloudStorageToBigQueryOperator,
)
from sness.pipeline.custom_operators.dataproc_operator import (
    build_ness_etl_path,
)
from sness.pipeline.custom_operators.minecraft_operator import (
    MinecraftOperator,
)
from sness.pipeline.slack import slack_failed_task, slack_miss_sla
from sness.settings.pod_type import PodType

DAG_NAME = "ExtractionHolidays"


def get_environment() -> str:
    if Variable.get("AIRFLOW_ENVIRONMENT") == "production":
        return Environment.PRODUCTION.value
    return Environment.STAGING.value


ENVIRONMENT = get_environment()


if ENVIRONMENT == Environment.STAGING.value:
    SLACK_NOTIFICATION_CHANNEL_ALERTS = (
        "alert_marketplace_analytics_dags_staging"
    )
    SCHEDULE_CRON = None
    RETRIES = 0

elif ENVIRONMENT == Environment.PRODUCTION.value:
    SLACK_NOTIFICATION_CHANNEL_ALERTS = "#alert_marketplace_analytics_dags"
    SCHEDULE_CRON = "0 5 * * 1"
    RETRIES = 2


default_args = {
    'owner': 'dc-marketplace-melhoria-continua',
    'depends_on_past': False,
    'retries': RETRIES,
    'retry_delay': timedelta(seconds=30),
    'start_date': datetime.strptime('2022-07-27', '%Y-%m-%d'),
    'on_failure_callback': slack_failed_task,
    'slack_notification_channel': SLACK_NOTIFICATION_CHANNEL_ALERTS,
    'params': {
        'labels': {
            'se_tribe': 'melhoria_continua',
            'se_vertical': 'operacoes_marketplace',
        }
    },
}


dag = DAG(
    DAG_NAME,
    catchup=False,
    default_args=default_args,
    schedule_interval=SCHEDULE_CRON,
    max_active_runs=1,
    sla_miss_callback=slack_miss_sla,
)


start = DummyOperator(task_id='Start', dag=dag)
end = DummyOperator(task_id='End', dag=dag)


extraction_holidays = MinecraftOperator(
    task_id='extraction_holidays',
    main=build_ness_etl_path(
        pipeline="extraction_holidays",
        zone="refined",
        name="holidays.py",
    ),
    max_instances=4,
    metadata={"PIP_PACKAGES": "bs4"},
    executor_size=PodType.EXECUTOR_SMALL,
    dag=dag,
)


if ENVIRONMENT == Environment.STAGING.value:
    start >> extraction_holidays >> end


elif ENVIRONMENT == Environment.PRODUCTION.value:
    tb_extraction_holidays_to_bq = CloudStorageToBigQueryOperator(
        task_id='extraction_holidays_to_bq',
        zone='refined',
        namespace='marketplace_analytics',
        dataset='tb_feriados_nacionais_estaduais_municipais',
        environment=Environment.PRODUCTION,
        dag=dag,
    )

    (start >> extraction_holidays >> tb_extraction_holidays_to_bq >> end)
