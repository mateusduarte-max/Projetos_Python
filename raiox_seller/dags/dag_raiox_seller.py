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

DAG_NAME = "RaioxSeller"


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
    SCHEDULE_CRON = "0 12 * * *"
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


blocklist_middle_topseller = MinecraftOperator(
    task_id='blocklist_middle_topseller',
    main=build_ness_etl_path(
        pipeline="raiox_seller",
        zone="refined",
        name="blocklist_middle_topseller.py",
    ),
    max_instances=4,
    metadata={"PIP_PACKAGES": "gspread oauth2client"},
    executor_size=PodType.EXECUTOR_SMALL,
    dag=dag,
)

catalog_middle_topseller = MinecraftOperator(
    task_id='catalog_middle_topseller',
    main=build_ness_etl_path(
        pipeline="raiox_seller",
        zone="refined",
        name="catalog_middle_topseller.py",
    ),
    max_instances=4,
    executor_size=PodType.EXECUTOR_SMALL,
    dag=dag,
)


if ENVIRONMENT == Environment.STAGING.value:
    start >> blocklist_middle_topseller >> end
    start >> catalog_middle_topseller >> end


elif ENVIRONMENT == Environment.PRODUCTION.value:
    tb_blocklist_termo_unico_carteira_to_bq = CloudStorageToBigQueryOperator(
        task_id='blocklist_termo_unico_carteira_to_bq',
        zone='refined',
        namespace='marketplace_analytics',
        dataset='tb_blocklist_termo_unico_carteira',
        environment=Environment.PRODUCTION,
        dag=dag,
    )

    tb_sellers_middle_topsellers_to_bq = CloudStorageToBigQueryOperator(
        task_id='sellers_middle_topsellers_to_bq',
        zone='refined',
        namespace='marketplace_analytics',
        dataset='tb_sellers_middle_topsellers',
        environment=Environment.PRODUCTION,
        dag=dag,
    )

    tb_catalog_middle_topseller_to_bq = CloudStorageToBigQueryOperator(
        task_id='catalog_middle_topseller_to_bq',
        zone='refined',
        namespace='marketplace_analytics',
        dataset='tb_catalogo_middle_topsellers',
        environment=Environment.PRODUCTION,
        dag=dag,
    )

    (
        start
        >> blocklist_middle_topseller
        >> [
            tb_blocklist_termo_unico_carteira_to_bq,
            tb_sellers_middle_topsellers_to_bq,
        ]
        >> end
    )
    (
        start
        >> catalog_middle_topseller
        >> tb_catalog_middle_topseller_to_bq
        >> end
    )
