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

DAG_NAME = "Fraud_Price"


def get_environment() -> str:
    if Variable.get("AIRFLOW_ENVIRONMENT") == "production":
        return Environment.PRODUCTION.value
    return Environment.STAGING.value


ENVIRONMENT = get_environment()


if ENVIRONMENT == Environment.STAGING.value:
    SLACK_NOTIFICATION_CHANNEL_ALERTS = "#canal_para_testes_em_staging"
    SCHEDULE_CRON = None
    RETRIES = 0

elif ENVIRONMENT == Environment.PRODUCTION.value:
    SLACK_NOTIFICATION_CHANNEL_ALERTS = "#alert_marketplace_analytics_dags"
    SCHEDULE_CRON = "0 8 * * *"
    RETRIES = 2


default_args = {
    'owner': 'dc-marketplace-melhoria-continua',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'start_date': datetime.strptime('2022-07-27', '%Y-%m-%d'),
    'on_failure_callback': slack_failed_task,
    'slack_notification_channel': '#alert_marketplace_analytics_dags',
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
    schedule_interval='@hourly',
    max_active_runs=1,
    sla_miss_callback=slack_miss_sla,
)


start = DummyOperator(task_id='Start', dag=dag)
end = DummyOperator(task_id='End', dag=dag)

"""
fraud_price = MinecraftOperator(
    task_id='fraud_price',
    main=build_ness_etl_path(
        pipeline="fraud_price",
        zone="refined",
        name="fraud_price.py",
    ),
    max_instances=4,
    executor_size=PodType.EXECUTOR_SMALL,
    dag=dag,
)"""

fraud_price_register = MinecraftOperator(
    task_id='fraud_price_register',
    main=build_ness_etl_path(
        pipeline="fraud_price",
        zone="refined",
        name="fraud_price_register.py",
    ),
    max_instances=4,
    executor_size=PodType.EXECUTOR_SMALL,
    dag=dag,
)


if ENVIRONMENT == Environment.STAGING.value:

    start >> fraud_price_register >> end


elif ENVIRONMENT == Environment.PRODUCTION.value:
    '''
    fraud_price_to_bq = CloudStorageToBigQueryOperator(
        task_id='fraud_price_to_bq',
        zone='refined',
        namespace='marketplace_analytics',
        dataset='fraud_price',
        environment=Environment.PRODUCTION,
        dag=dag,
    )'''

    fraud_price_register_to_bq = CloudStorageToBigQueryOperator(
        task_id='fraud_price_register_to_bq',
        zone='refined',
        namespace='marketplace_analytics',
        dataset='tb_fraude_pricing_cadastro',
        environment=Environment.PRODUCTION,
        dag=dag,
    )

    start >> fraud_price_register >> fraud_price_register_to_bq >> end
