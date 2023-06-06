from pyspark.sql import DataFrame
from sness.datalake.metadata import DatasetMetadata, Environment, Zone
from sness.datalake.sness_spark import SnessSpark

ss = SnessSpark()


def query_view_bq(dataset_name: str) -> DataFrame:
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
        .cache()
    )

    return df


# Escrita - Zona REFINED
def write_to_refined(
    df_catalogo: DataFrame,
    ss: SnessSpark,
    environment: Environment,
    namespace: str,
    dataset_name: str,
) -> None:
    """
    Escrita no Lake
    ____________________________
    Parâmetros:
        df_catalogo: dataset a ser gravado
        ss: SnessSpark
        READ_ENVIRONMENT:Environment
        namespace:namespace a ser escrito
        dataset:dataset a ser escrito
    """
    ss.write.parquet(
        df=df_catalogo,
        environment=environment,
        zone=Zone.REFINED,
        namespace=namespace,
        dataset=dataset_name,
        mode="overwrite",
    )


def main() -> None:

    READ_WRITE_ENVIRONMENT = Environment.PRODUCTION

    df_catalogo = query_view_bq(
        "maga-bigdata.marketplace_analytics.view_catalogo_middle_topsellers"
    )

    DATASET = DatasetMetadata(
        namespace="marketplace_analytics",
        dataset="tb_catalogo_middle_topsellers",
    )

    write_to_refined(
        df_catalogo,
        ss,
        READ_WRITE_ENVIRONMENT,
        DATASET.namespace,
        DATASET.dataset,
    )

    print("ETL Finalizado!")


if __name__ == "__main__":

    main()
