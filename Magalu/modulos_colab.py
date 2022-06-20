# Importar biblioteca biqquery
import pandas as pd
from google.cloud import bigquery
from google.colab import auth
auth.authenticate_user()
import gspread
from google.auth import default
creds, _ = default()
gc = gspread.authorize(creds)
from gspread_dataframe import set_with_dataframe

class dados:
  
  def consulta_bigquery(projeto, select):
    """ Função para consultar BigQuery"""
    maga_bigdata = projeto
    client = bigquery.Client(project=maga_bigdata)
    sql = select
    df = client.query(sql).to_dataframe().fillna('')
    return df


  def grava_tabela_drive(nome_tabela, df):
    """ Função para gravar na tabela Drive"""
    #Abrir planilha e primeira aba
    pagina = gc.open(nome_tabela).sheet1
    #Limpar base
    pagina.clear()
    #Importar dataframe na planilha
    return set_with_dataframe(pagina, df, include_column_header=True)


  def busca_tabela_drive(nome_tabela, sheet):
    """Busca tabela no google drive"""
    # abrir arquivo google
    arquivo = gc.open(nome_tabela)
    # abrir planilha 'aba'
    pag_tabela = arquivo.worksheet(sheet)
    # colocar a aba dentro de um data_frame
    return pd.DataFrame(pag_tabela.get_all_records())





  