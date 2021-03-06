import pandas as pd
import pyodbc
import datetime 


# Conecta no banco Sql Server
cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                        "Server=172.16.198.91;"
                        "Database=I3_IC;"
                        "uid=Magazine luiza;pwd=Flex@2020")


# Data anterior
def dia_anterior():
    date_now = datetime.datetime.now()
    dia_anterior = date_now - datetime.timedelta(days=1)
    dia_anterior_1 = dia_anterior.strftime("%y-%m-%d")
    dia_anterior_2 = "20"+dia_anterior_1
    return dia_anterior_2

dia_anterior()

# Consulta banco
def consulta_banco():
    consulta = pd.read_sql_query("Select CallId AS IdentificadordaChamada, CallType AS TipodaChamada, "
                        "CallDirection AS DirecaodaChamada, StationId AS EstacaodeTrabalho, LocalUserId AS Operador, "
                        "AssignedWorkGroup AS FiladeAtendimento, RemoteNumber AS NumeroCliente, "
                        "InitiatedDate AS DataInicio, ConnectedDate AS DataConexao, TerminatedDate AS DataFinalizacao, "
                        "CallDurationSeconds AS DuracaodaChamada, HoldDurationSeconds AS TempoemHold, Dnis AS DDRdaURA, "
                        "CallEventLog AS LogdaChamada, CustomString1 AS CPF, CustomString2 AS Ticket, "
                        "CallNote AS InformacoesdaUra "
                        "FROM [dbo].[calldetail_viw] WITH(NOLOCK) "
                        "where InitiatedDate between  '{} 00:00:00.00' and '{} 23:59:59.00'".format(dia_anterior(), dia_anterior()), cnxn)
    df["Quantidade"] = 1
    return consulta 

df = consulta_banco()



def operador(df):      # Cria função validação operador
    if ("-" in df):
        return 'N'
    else:
        return 'SAC'
df['Val_operador'] = df['Operador'].apply(lambda x: operador(x))   # Criar coluna de validação



def fila(df):
    if ("-" in df):
        return 'N'
    else:
        return 'SAC'
df['Val_fila'] = df['FiladeAtendimento'].apply(lambda x: fila(x))   # Criar coluna de validação



def ramal(df):      # Cria função validação ramal
    if ("1186" in df):
        return 'SAC'
    elif ("1188" in df):
        return 'SAC'
    elif ("1189" in df):
        return 'SAC'
    elif ("47651215" in df):
        return 'SAC'
    elif ("47651210" in df):
        return 'SAC'
    elif ("47651211" in df):
        return 'SAC'
    elif ("0603" in df):
        return 'SAC'
    elif ("0601" in df):
        return 'SAC'
    elif ("0602" in df):
        return 'SAC'
    else:
        return 'N'
df['Val_ramal'] = df['DDRdaURA'].apply(lambda x: ramal(x))   # Criar coluna de validação


df['Val_cpf'] = df['CPF'].str.len()    # Cria coluna com contagem cpf
df['Validacao_geral'] = df['Val_operador'] +'_' + df['Val_fila'] +'_' + df['Val_ramal']  # Cria coluna com validação geral


df2 = df.query('Val_cpf==11')
df3 = df2.set_index('Validacao_geral')
df4 = df3.loc[['SAC_N_SAC', 'SAC_SAC_SAC' ], ['Operador', 'FiladeAtendimento', 'CPF','Quantidade']]




def cpf_anonimo(df4):      # Cria função cpf anonimo
    if ("9999999999" in df4):
        return "Anonimo"
    else:
        return "Valido"
df4['CPF_2'] = df4['CPF'].apply(lambda x: cpf_anonimo(x))   # Criar coluna de validação


df5 = df4.query('CPF_2=="Valido"')


contato = df5['CPF'].count()
df6 = df5.drop_duplicates(subset='CPF', keep="last")
contato_2 = df6['CPF'].count()

recontato = contato-contato_2
recontato_2 = round(((recontato/contato)*100),2)
print(recontato_2)



# Caminho para salvar arquivo
#caminho = r"C:\Users\mo_duarte\Desktop\RESOLVE\Indicadores\Ura\contatos_{}.xlsx".format(dia_anterior_2)

# Export dataframe
#df.to_excel(caminho)