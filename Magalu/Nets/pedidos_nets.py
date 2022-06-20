import cx_Oracle
import pandas as pd
from google.oauth2 import service_account
import datetime
from autenticacoes_nets import Connection

con = Connection()

dsn_tns = cx_Oracle.makedsn(con.ip, con.port, con.SID)

connection = cx_Oracle.connect(con.usuario, con.senha, dsn_tns)

data_base = (datetime.datetime.now() - datetime.timedelta(45)).strftime('%d-%m-%Y')

df_ora = pd.read_sql_query("""WITH MKT AS(
  SELECT 
    DISTINCT 
    NOTF.COD_PEDIDO_LOJA,
    NOTF.COD_PEDIDO,
    SID_TEMPO_PEDIDO_LOJA DAT_CRIA,
    PGT.SID_TEMPO DAT_APROV,
    UNIN.DES_UNI_NEG,
    PEDS_QTF_PRZDIAPAG PRAZO,
    STAP.STAP_NOM STAP,
    ALMO.NOM_ALMOXA,
    NOTF.VLR_LIQ_PEDIDO,
    PEDS.CLIF_COD,
    PED.DAT_CRIA_PEDIDO,
    PED.VLR_FRETE_CLI
  FROM NS_DWMP.TFAT_PEDIDO_NOTFIS NOTF
  JOIN NS_DWMP.TDIM_ALMOXA ALMO ON ALMO.SID_ALMOXA = NOTF.SID_ALMOXA
  JOIN NS_DWMP.TFAT_PAGTO_PEDIDO PGT ON NOTF.SID_PEDIDO = PGT.SID_PEDIDO
  JOIN NS_DWMP.TDIM_UNI_NEG UNIN ON UNIN.SID_UNI_NEG = NOTF.SID_UNI_NEG
  JOIN BI_STAGE.ABACOS_TCOM_PEDSAI PEDS ON PEDS.PEDS_COD = NOTF.COD_PEDIDO
  JOIN BI_STAGE.ABACOS_TCOM_STAPDS STAP ON PEDS.STAP_COD=STAP.STAP_COD
  FULL JOIN NS_DWMP.TDIM_PEDIDO PED ON NOTF.SID_PEDIDO = PED.SID_PEDIDO
  WHERE NOTF.IND_STATUS_PEDIDO_LOJA <> 'T'
  AND NOTF.SID_PAIS = 1
  AND PED.DAT_CRIA_PEDIDO >= TO_DATE('{}','DD/MM/YY')
  AND ALMO.FLG_MARKET_PLACE =1
  AND PGT.SID_TEMPO <> -1
  AND STAP.STAP_COD IN (10,11,12,27,28)
  --AND NOM_ALMOXA = 'Studio Z Calçados'
  
  
  --AND ALMO.NOM_ALMOXA in ('ELLA STORE', 'Santa Fé', '3LS3 CALÇADOS', 'Ded Calçados', 'LandFeet', 'Acero', 'Calçados Pastori', 'BmBrasil', 'Músculos na Web', 'Ishoes', 'CentralFit', 'BNC SUPLEMENTOS', 'Alma de Praia', 'Alex Shoes', 'PIXOLE CALÇADOS', 'Tucca Calçados')
  ), 

MKT2 AS(

SELECT
CLI.CLIF_COD,
EST.ESTF_SIG,
ENT.ENTE_CEP AS CEP,
EST.ESTF_NOM AS ESTADO,
EST.ESTF_SIG AS SIGLA,
MUN.MUNC_END_MUN AS MUNICIPIO
FROM BI_STAGE.ABACOS_TCOM_CLIFOR CLI
  JOIN MKT ON CLI.CLIF_COD = MKT.CLIF_COD
  JOIN BI_STAGE.ABACOS_TGEN_ENTEND ENT ON  ENT.ENTB_COD = CLI.ENTB_COD
  JOIN BI_STAGE.ABACOS_TGEN_MUNCID MUN ON ENT.MUNC_COD=MUN.MUNC_COD
  JOIN BI_STAGE.ABACOS_TGEN_ESTFED EST ON MUN.ESTF_COD=EST.ESTF_COD
),

LOJS AS (
SELECT 
a.LOJS_NOM, 
a.LOJS_EXT_COD SELLER
--a.LOJS_DAT_ULT_ALT
FROM BI_STAGE.ABACOS_tmkp_lojsta a
WHERE LOJS_DAT_ULT_ALT = (SELECT MAX(b.LOJS_DAT_ULT_ALT)
                            FROM BI_STAGE.ABACOS_tmkp_lojsta b
                            WHERE b.LOJS_NOM = a.LOJS_NOM
                            GROUP BY b.LOJS_NOM)) --LOJS ON LOJS.LOJS_NOM = ALMO.NOM_ALMOXA

                
SELECT 
  NOM_ALMOXA LOJISTA,
  LOJS.SELLER ID_SELLER,
  COD_PEDIDO_LOJA,
  COD_PEDIDO,
  MAX(CASE WHEN STAP_COD = 27 THEN TO_CHAR(LOGP_DAT_CAD, 'YYYY/MM/DD') ELSE NULL END) DATA_DESPACHO,
  MAX(CASE WHEN STAP_COD = 28 THEN TO_CHAR(LOGP_DAT_CAD, 'YYYY/MM/DD') ELSE NULL END) DATA_ENTREGA,
  MAX(MKT.DAT_CRIA) DATA_PEDIDO,
  MAX(MKT.DAT_APROV) DATA_APROV,
  MKT.DAT_CRIA_PEDIDO,
  DES_UNI_NEG UNI_NEG,
  MAX(PRAZO) DIAS_PRAZO,
  MKT.STAP STATUS,
  MKT2.CEP,
  MKT2.ESTADO,
  MKT2.SIGLA,
  MKT2.MUNICIPIO,
  MKT.VLR_LIQ_PEDIDO VLR,
  MKT.VLR_FRETE_CLI FRETE
FROM BI_STAGE.ABACOS_TCOM_LOGPDS LOGP
JOIN MKT ON MKT.COD_PEDIDO = LOGP.PEDS_COD
JOIN LOJS ON LOJS.LOJS_NOM = MKT.NOM_ALMOXA
JOIN MKT2 ON MKT.CLIF_COD=MKT2.CLIF_COD
GROUP BY 
MKT.DES_UNI_NEG,
COD_PEDIDO_LOJA, 
NOM_ALMOXA,
LOJS.SELLER,
COD_PEDIDO, 
MKT.STAP, 
MKT.VLR_LIQ_PEDIDO,
MKT.VLR_FRETE_CLI,
MKT.DAT_CRIA_PEDIDO,
MKT2.CEP,
MKT2.ESTADO,
MKT2.SIGLA,
MKT2.MUNICIPIO""".format(data_base), con=connection)

# Criar autenticação
# Gravar na tabela
credentials = service_account.Credentials.from_service_account_file(r'C:\Users\mo_duarte\Desktop\Marketplace\chave_google\marketplace-analytics-333712-5416677751e5.json')
df_ora.to_gbq(destination_table='data.pedidos_nets',project_id='marketplace-analytics-333712', if_exists='replace', credentials=credentials, progress_bar=True)

#df_ora.to_excel(r'C:\Users\mo_duarte\Desktop\Marketplace\pedidos\Base_pedidos_nets\studio_z.xlsx')

print("Terminou!!!")
