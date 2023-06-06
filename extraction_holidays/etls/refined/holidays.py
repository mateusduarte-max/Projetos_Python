import pandas as pd
import requests
from bs4 import BeautifulSoup
from pyspark.sql import DataFrame
from sness.datalake.metadata import DatasetMetadata, Environment, Zone
from sness.datalake.sness_spark import SnessSpark

ss = SnessSpark()


def write_to_refined_holidays(
    df_holidays_spark: DataFrame,
    ss: SnessSpark,
    environment: Environment,
    namespace: str,
    dataset_name: str,
) -> None:
    """
    Escrita no Lake
    ____________________________
    Parâmetros:
        dataset final: dataset a ser gravado
        ss: SnessSpark
        READ_ENVIRONMENT:Environment
        namespace:namespace a ser escrito
        dataset:dataset a ser escrito
    """
    ss.write.parquet(
        df=df_holidays_spark,
        environment=environment,
        zone=Zone.REFINED,
        namespace=namespace,
        dataset=dataset_name,
        mode="overwrite",
    )


def extraction_holidays(url: str):
    df = requests.get(url).json()
    df_counties = pd.DataFrame(df)
    df_counties = df_counties[["municipio-nome", "UF-sigla"]]
    df_counties.rename(
        columns={'municipio-nome': 'municipios', 'UF-sigla': 'estado'},
        inplace=True,
    )

    df_counties['municipios'] = (
        df_counties['municipios']
        .str.normalize('NFKD')
        .str.encode('ascii', errors='ignore')
        .str.decode('utf-8')
        .str.lower()
        .str.replace(' ', '_')
    )

    df_counties['estado'] = df_counties['estado'].str.lower()
    counties = df_counties['municipios'].to_list()
    estate = df_counties['estado'].to_list()

    df_holidays = []
    for c, e in zip(counties, estate):
        html = requests.get(
            f"https://www.feriados.com.br/feriados-{c}-{e}.php?ano=2023"
        ).content
        soup = BeautifulSoup(html, 'html.parser')
        holidays = [
            t.text.strip()
            for t in soup.find_all(
                "span", attrs={'class': 'style_lista_feriados'}
            )
        ]
        df = pd.DataFrame(holidays, columns=['col'])
        divisao = df["col"].str.split("-", n=1, expand=True)
        df["Data"] = divisao[0]
        df["Desricao"] = divisao[1]
        df['municipio'] = c
        df['estado'] = e
        df_holidays.append(pd.DataFrame(df))

    df_holidays = pd.concat(df_holidays)

    return df_holidays


def main() -> None:

    READ_WRITE_ENVIRONMENT = Environment.PRODUCTION

    df_holidays = extraction_holidays(
        "https://servicodados.ibge.gov.br/api/v1/localidades/municipios?view=nivelado"
    )

    df_holidays_spark = ss.spark.createDataFrame(df_holidays)

    DATASET = DatasetMetadata(
        namespace="marketplace_analytics",
        dataset="tb_feriados_nacionais_estaduais_municipais",
    )

    write_to_refined_holidays(
        df_holidays_spark,
        ss,
        READ_WRITE_ENVIRONMENT,
        DATASET.namespace,
        DATASET.dataset,
    )


if __name__ == "__main__":

    main()
