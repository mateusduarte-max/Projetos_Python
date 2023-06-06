from pyspark.sql import DataFrame
from sness.datalake.metadata import DatasetMetadata, Environment, Zone
from sness.datalake.sness_spark import SnessSpark

ss = SnessSpark()


def query_fraud_price(dataset_name: str) -> DataFrame:
    """Consulta View BQ
    ___________________________________________
    Parâmetros:
        dataset_name: nome dataset
    """
    df = (
        ss.spark.read.format("bigquery")
        .option("viewsEnabled", True)
        .option("table", dataset_name)
        .load()
    )
    return df


def write_to_refined(
    df_fraud_price: DataFrame,
    ss: SnessSpark,
    environment: Environment,
    namespace: str,
    dataset_name: str,
) -> None:
    """
    Escrita no Lake
    ____________________________
    Parâmetros:
        df_fraud_price: dataset a ser gravado
        ss: SnessSpark
        READ_ENVIRONMENT:Environment
        namespace:namespace a ser escrito
        dataset:dataset a ser escrito
    """
    ss.write.parquet(
        df_fraud_price,
        environment=environment,
        zone=Zone.REFINED,
        namespace=namespace,
        dataset=dataset_name,
        mode="overwrite",
    )


def main() -> None:

    READ_WRITE_ENVIRONMENT = Environment.PRODUCTION

    df_fraud_price = query_fraud_price(
        "maga-bigdata.marketplace_analytics.view_fraud_price"
    )

    DATASET = DatasetMetadata(
        namespace="marketplace_analytics",
        dataset="fraud_price",
    )

    write_to_refined(
        df_fraud_price,
        ss,
        READ_WRITE_ENVIRONMENT,
        DATASET.namespace,
        DATASET.dataset,
    )

    print("ETL Finalizado")


if __name__ == "__main__":

    main()
