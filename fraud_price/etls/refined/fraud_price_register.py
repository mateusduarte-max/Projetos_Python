from datetime import datetime, timedelta
from functools import reduce

import pyspark.sql.functions as sf
import pyspark.sql.types as st
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from sness.datalake.metadata import DatasetMetadata, Environment, Zone
from sness.datalake.sness_spark import SnessSpark
from sness.etl.spark_utils import download_json_from_gcs

ss = SnessSpark()

# Formatar data
d_1 = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
d_0 = (datetime.now()).strftime("%Y%m%d")


def register() -> DataFrame:
    """
    Dados do cadastro (catalogo)
    ______________________________
    """
    dfRegisterYesterday = (
        ss.spark.read.format("bigquery")
        .option("table", f"maga-bigdata.catalogo.product_{d_1}")
        .load()
        .withColumn("cat", sf.arrays_zip("categories"))
        .withColumn("cat", sf.explode("cat"))
        .withColumn("sub", sf.explode("cat.categories.subcategories"))
        .select(
            sf.col("seller_id"),
            sf.col("created_at").alias("data_atualizacao_sku"),
            sf.col("sku"),
            sf.col("navigation_id"),
            sf.col("niagara_timestamp"),
            sf.col("brand"),
            sf.col("cat.categories.id").alias("categoria"),
            sf.col("sub.id").alias("subcategoria"),
            sf.col("offer_title").alias("titulo"),
        )
    )

    dfRegisterToday = (
        ss.spark.read.format("bigquery")
        .option("table", f"maga-bigdata.catalogo.product_{d_0}")
        .load()
        .withColumn("cat", sf.arrays_zip("categories"))
        .withColumn("cat", sf.explode("cat"))
        .withColumn("sub", sf.explode("cat.categories.subcategories"))
        .select(
            sf.col("seller_id"),
            sf.col("created_at").alias("data_atualizacao_sku"),
            sf.col("sku"),
            sf.col("navigation_id"),
            sf.col("niagara_timestamp"),
            sf.col("brand"),
            sf.col("cat.categories.id").alias("categoria"),
            sf.col("sub.id").alias("subcategoria"),
            sf.col("offer_title").alias("titulo"),
        )
    )

    df_register_tt = reduce(
        DataFrame.unionAll, [dfRegisterYesterday, dfRegisterToday]
    )

    windowSpec = Window.partitionBy(
        "seller_id",
        "data_atualizacao_sku",
        "sku",
        "navigation_id",
        "brand",
        "categoria",
        "subcategoria",
        "titulo",
    ).orderBy("niagara_timestamp")

    df_register_tt = df_register_tt.withColumn(
        "row_number", sf.row_number().over(windowSpec)
    )

    df_register_tt = df_register_tt.distinct()

    return df_register_tt


def price() -> DataFrame:
    """
    Dados de price
    ______________________________
    """
    dfPriceYesterday = (
        ss.spark.read.format("bigquery")
        .option("table", f"maga-bigdata.pricing.updatedprice_{d_1}")
        .load()
        .select(
            sf.col("seller_id").alias("seller_id_p"),
            sf.col("sku").alias("sku_p"),
            sf.col("price"),
        )
    )

    dfPriceToday = (
        ss.spark.read.format("bigquery")
        .option("table", f"maga-bigdata.pricing.updatedprice_{d_0}")
        .load()
        .select(
            sf.col("seller_id").alias("seller_id_p"),
            sf.col("sku").alias("sku_p"),
            sf.col("price"),
        )
    )

    df_price_tt = reduce(DataFrame.unionAll, [dfPriceYesterday, dfPriceToday])

    df_price_tt = df_price_tt.distinct()

    return df_price_tt


def average_sale() -> DataFrame:
    """
    Média de vendas últimos 15dias
    ______________________________
    """
    df_sale = (
        ss.read.parquet(
            Environment.PRODUCTION,
            Zone.TRUSTED,
            'apolo',
            'fact_vendas_captadas',
        )
        .select(
            sf.col("categoria").alias("cat_media"),
            sf.col("subcategoria").alias("subcat_media"),
            sf.col("vlvendaitem"),
        )
        .where(
            "TO_DATE(CAST(datapedidokey AS STRING), 'yyyyMMdd') >= CURRENT_DATE()-15 \
                                                                       AND DATE(datapagamentomktp) IS NOT NULL \
                                                                       AND UPPER(STATUS) <> 'TESTE' \
                                                                       AND UPPER (IFNULL(IDSELLERMARKETPLACE,'-1')) <> 'MAGAZINELUIZA'"
        )
    )

    df_sale = df_sale.groupby("cat_media", "subcat_media").agg(
        sf.avg("vlvendaitem").alias("valor_medio_cat_subcat")
    )

    df_sale = df_sale.distinct()

    return df_sale


def safra() -> DataFrame:
    """
    Dados referente a Safra Seller
    ______________________________
    """
    dfSafra = (
        ss.spark.read.format("bigquery")
        .option("table", "maga-bigdata.bi_ecomm.TB_SafraSellers")
        .load()
        .select(
            sf.col("SellerID").alias("sellerid_s"),
            sf.col("Data_Publicado").alias("data_publicado"),
            sf.col("Carteira").alias("carteira"),
            sf.col("Status").alias("status"),
            sf.col("Plataforma").alias("plataforma"),
        )
    )
    dfSafra = dfSafra.distinct()

    return dfSafra


def join_dfs(
    df_register_tt: DataFrame,
    df_price_tt: DataFrame,
    df_sale: DataFrame,
    df_safra: DataFrame,
) -> DataFrame:
    """
    Joins dfs
    ______________________________
    Parâmetros:
        df_register_tt: DataFrame
        df_price_tt: DataFrame
        df_sale: DataFrame
        df_safra: DataFrame

    """
    dfJoin = df_register_tt.join(
        df_price_tt,
        how="left",
        on=[
            df_register_tt.seller_id == df_price_tt.seller_id_p,
            df_register_tt.sku == df_price_tt.sku_p,
        ],
    )

    dfJoin = dfJoin.where("row_number==1 and price is not null")

    dfJoin = dfJoin.join(
        df_sale,
        how='left',
        on=[
            dfJoin.categoria == df_sale.cat_media,
            dfJoin.subcategoria == df_sale.subcat_media,
        ],
    ).join(df_safra, how='left', on=[dfJoin.seller_id == df_safra.sellerid_s])

    dfJoin = dfJoin.drop('seller_id_p', 'sku_p', 'sellerid_s')

    dfJoin = dfJoin.groupby(
        'seller_id',
        'data_atualizacao_sku',
        'sku',
        'navigation_id',
        'niagara_timestamp',
        'brand',
        'categoria',
        'subcategoria',
        'titulo',
        'row_number',
        'data_publicado',
        'carteira',
        'status',
        'plataforma',
    ).agg(
        sf.sum('price').alias('price'),
        sf.sum('valor_medio_cat_subcat').alias('valor_medio_cat_subcat'),
    )

    dfJoin = dfJoin.withColumn(
        "IP", (sf.col("price") / sf.col("valor_medio_cat_subcat"))
    )

    dfJoin = dfJoin.where(
        "IP <0.70 \
                            AND carteira NOT IN ('Verticais','Top Sellers','Key Account','Loja do grupo')\
                            AND price >200.00"
    )

    dfJoin = dfJoin.withColumn("update", sf.current_timestamp().cast("string"))

    dfJoin = dfJoin.distinct()

    dfJoin = dfJoin.sort(['data_publicado'], ascending=False)

    return dfJoin


# Escrita - Zona REFINED
def write_to_refined(
    dfJoin: DataFrame,
    ss: SnessSpark,
    environment: Environment,
    namespace: str,
    dataset_name: str,
) -> None:
    """
    Escrita no Lake
    ____________________________
    Parâmetros:
        dfJoin: dataset a ser gravado
        ss: SnessSpark
        READ_ENVIRONMENT:Environment
        namespace:namespace a ser escrito
        dataset:dataset a ser escrito
    """
    ss.write.parquet(
        df=dfJoin,
        environment=environment,
        zone=Zone.REFINED,
        namespace=namespace,
        dataset=dataset_name,
        mode="overwrite",
    )


def main() -> None:

    ss = SnessSpark()

    READ_WRITE_ENVIRONMENT = Environment.PRODUCTION

    df_register_tt = register()

    df_price_tt = price()

    df_sale = average_sale()

    df_safra = safra()

    dfJoin = join_dfs(df_register_tt, df_price_tt, df_sale, df_safra)

    DATASET = DatasetMetadata(
        namespace="marketplace_analytics",
        dataset="tb_fraude_pricing_cadastro",
    )

    write_to_refined(
        dfJoin,
        ss,
        READ_WRITE_ENVIRONMENT,
        DATASET.namespace,
        DATASET.dataset,
    )

    print("Tabela rebate geral, atualizada!")


if __name__ == "__main__":

    main()
