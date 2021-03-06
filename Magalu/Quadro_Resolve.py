# importar biblioteca
import pandas as pd   
import datetime as dt

# Dia atual
def dia():
    date_now = dt.datetime.now() # Busca data atual
    dia_atual = date_now.strftime("%y-%m-%d") # Formata a data
    return dia_atual

# Importar base
def importar_base_quadro():
    arquivo = "C:\\Users\\mo_duarte\\Desktop\\RESOLVE\\Indicadores\\QuadroAtivo\\Funcionarios Ativos.xls"
    quadro = pd.read_table(arquivo, encoding='cp1252')
    quadro['COD_CENTRO_CUSTO'] = quadro['COD_CENTRO_CUSTO'].astype(object)   # transformar tipo de coluna
    # Formatar coluna de data
    quadro['DATA_ADMISSAO']  = pd.to_datetime(quadro['DATA_ADMISSAO'])
    quadro['Data_Adimissão'] = quadro['DATA_ADMISSAO'].dt.strftime('%d-%m-%Y')
    quadro.to_excel(r'\\fs\Atendimento\documentos\Planejamento\Pessoas\Controle de RPs\Func. Ativos\Quadro_Empresa_{}.xlsx'.format(dia()), index=False)
    # visualização base
    return quadro
#Instânciar função
quadro = importar_base_quadro()

# CR Luiza Resolve
def coluna_centro_custo():
    def centro_custo(quadro):                                  
        if quadro['COD_CENTRO_CUSTO'] == 90601:
            return "Resolve" 
        elif quadro['COD_CENTRO_CUSTO'] == 90602:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 90603:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 90604:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 90605:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 90606:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 90607:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 90608:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130101:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130102:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130103:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130104:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130105:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130106:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130107:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130108:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130109:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130110:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130201:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130202:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130203:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130204:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130301:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130302:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130303:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130401:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130402:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130403:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130404:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130405:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130406:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130407:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130408:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130409:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130410:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130411:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130412:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130413:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130501:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130502:
            return "Resolve"
        elif quadro['COD_CENTRO_CUSTO'] == 130601:
            return "Resolve"
        else:
            return ""
    quadro['CR'] = quadro.apply(centro_custo, axis=1)    # Função aplicada
    return quadro    # Visualização


quadro = coluna_centro_custo()

# Filtro Quadro Luiza Resolve
def filtra_quadro_resolve():
    quadro.set_index(['CR'])   # indexar coluna
    resolve = quadro['CR'] == 'Resolve'   # selecionar CR resolve
    quadro_resolve = quadro[resolve]     # filtrar CR
    return quadro_resolve


quadro_resolve = filtra_quadro_resolve()

# CR Luiza Resolve
def coluna_centro_detalhe():
    def centro_custo_detalhe(quadro_resolve):                                  
        if quadro_resolve['COD_CENTRO_CUSTO'] == 90601:
            return "Magalu" 
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 90602:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 90603:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 90604:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 90605:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 90606:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 90607:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 90608:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130101:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130102:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130103:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130104:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130105:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130106:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130107:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130108:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130109:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130110:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130201:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130202:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130203:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130204:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130301:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130302:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130303:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130401:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130402:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130403:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130404:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130405:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130406:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130407:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130408:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130409:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130410:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130411:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130412:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130413:
            return "Magalu"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130501:
            return "Nets"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130502:
            return "Nets"
        elif quadro_resolve['COD_CENTRO_CUSTO'] == 130601:
            return "Epoca"
        else:
            return ""
    quadro_resolve_2 = quadro_resolve.copy()
    quadro_resolve_2['CR_detalhe'] = quadro_resolve_2.apply(centro_custo_detalhe, axis=1)    # Função aplicada
    quadro_resolve_2.to_excel(r'C:\Users\mo_duarte\Desktop\RESOLVE\Indicadores\QuadroAtivo\Quadro_Resolve_{}.xlsx'.format(dia()), index=False)
    quadro_resolve_2.to_excel(r'\\fs\Atendimento\documentos\Planejamento\Pessoas\Controle de RPs\Func. Ativos\Quadro_Resolve_{}.xlsx'.format(dia()), index=False)
    return print(quadro_resolve_2)  # Visualização


coluna_centro_detalhe()



