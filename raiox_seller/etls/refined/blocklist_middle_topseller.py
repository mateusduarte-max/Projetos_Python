# pip3 install gspread
# pip3 install oauth2client

import gspread
import pyspark.sql.functions as sf
import pyspark.sql.types as st
from oauth2client.service_account import ServiceAccountCredentials
from pyspark import SparkContext
from pyspark.sql import DataFrame
from sness.datalake.metadata import DatasetMetadata, Environment, Zone
from sness.datalake.sness_spark import SnessSpark
from sness.etl.spark_utils import download_json_from_gcs

ss = SnessSpark()

sc = SparkContext.getOrCreate()


def read_spreadsheet_data(auth_json, file_id, sheet_name):
    creds = download_json_from_gcs(auth_json)
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(
        creds, scope
    )
    gc = gspread.authorize(credentials)
    spreadsheet = gc.open_by_key(file_id)
    worksheet = spreadsheet.worksheet(sheet_name)
    data = worksheet.get_all_values()

    schema = st.StructType(
        [
            st.StructField("SellerID", st.StringType(), True),
            st.StructField("Carteira", st.StringType(), True),
            st.StructField("Responsavel", st.StringType(), True),
            st.StructField("Mundo", st.StringType(), True),
        ]
    )
    rdd = sc.parallelize(data[1:])
    df_sellers_carteira = ss.spark.createDataFrame(rdd, schema)

    return df_sellers_carteira


def query_bq(dataset_name: str) -> DataFrame:
    """Consulta View BQ
    ___________________________________________
    Parâmetros:
        dataset_name: nome dataset
    """
    df = (
        ss.spark.read.format("bigquery")
        .option("table", dataset_name)
        .load()
        .cache()
    )

    return df


def join_df(
    df_sellers_carteira: DataFrame, df_blocklist: DataFrame
) -> DataFrame:

    df_join = df_sellers_carteira.join(
        df_blocklist,
        on=[df_sellers_carteira.SellerID == df_blocklist.seller],
        how='inner',
    )
    return df_join


# Escrita - Zona REFINED
def write_to_refined(
    df_sellers_blocklist: DataFrame,
    ss: SnessSpark,
    environment: Environment,
    namespace: str,
    dataset_name: str,
) -> None:
    """
    Escrita no Lake
    ____________________________
    Parâmetros:
        df_sellers_blocklist: dataset a ser gravado
        ss: SnessSpark
        READ_ENVIRONMENT:Environment
        namespace:namespace a ser escrito
        dataset:dataset a ser escrito
    """
    ss.write.parquet(
        df=df_sellers_blocklist,
        environment=environment,
        zone=Zone.REFINED,
        namespace=namespace,
        dataset=dataset_name,
        mode="overwrite",
    )


# Escrita - Zona REFINED
def write_to_refined_sellers(
    df_sellers_carteira: DataFrame,
    ss: SnessSpark,
    environment: Environment,
    namespace: str,
    dataset_name: str,
) -> None:
    """
    Escrita no Lake
    ____________________________
    Parâmetros:
        df_sellers_carteira: dataset a ser gravado
        ss: SnessSpark
        READ_ENVIRONMENT:Environment
        namespace:namespace a ser escrito
        dataset:dataset a ser escrito
    """
    ss.write.parquet(
        df=df_sellers_carteira,
        environment=environment,
        zone=Zone.REFINED,
        namespace=namespace,
        dataset=dataset_name,
        mode="overwrite",
    )


def main() -> None:

    READ_WRITE_ENVIRONMENT = Environment.PRODUCTION

    df_sellers_carteira = read_spreadsheet_data(
        "gs://prd-lake-safe-marketplace_analytics/credentials/service_account_credentials.json",
        "1h7eiHg7qURU-EpQ9DOmYZtGZiW_D0OVAd-4kAsDk3xU",
        "base",
    )

    df_blocklist = query_bq(
        "maga-bigdata.marketplace_analytics.sku_termo_unico"
    )

    df_sellers_blocklist = join_df(df_sellers_carteira, df_blocklist)

    DATASET = DatasetMetadata(
        namespace="marketplace_analytics",
        dataset="tb_blocklist_termo_unico_carteira",
    )

    write_to_refined(
        df_sellers_blocklist,
        ss,
        READ_WRITE_ENVIRONMENT,
        DATASET.namespace,
        DATASET.dataset,
    )

    DATASET = DatasetMetadata(
        namespace="marketplace_analytics",
        dataset="tb_sellers_middle_topsellers",
    )

    write_to_refined_sellers(
        df_sellers_carteira,
        ss,
        READ_WRITE_ENVIRONMENT,
        DATASET.namespace,
        DATASET.dataset,
    )

    print("ETL Finalizado!")


if __name__ == "__main__":

    main()
