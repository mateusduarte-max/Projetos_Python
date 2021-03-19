import pandas as pd
import datetime as dt

# Função formatar base sac
def base_sac():
    base = pd.read_excel(r'C:\Users\mo_duarte\Desktop\RESOLVE\Indicadores\ResgateJa\BaseResgateJá\2021\01-2021\Resgate_Ja_SAC_21_01_2021.xlsx', header=2)
    def pos_mkt(base):
      if base['TIPO_DESPESA'] == 'RESGATE JA SAC ':
          return '3P'
      elif base['TIPO_DESPESA'] == 'RESTITUICAO DE CLIENTES;.;.;.;.':
          return 'Restituição'
      elif base['TIPO_DESPESA'] == 'RESTITUICAO DE CLIENTES MKTPLACE':
          return 'Restituição' 
      else:
          return '1P'
    base['Canal'] = base.apply(pos_mkt, axis=1)    # Função aplicada   
    return base 

sac = base_sac() # Instanciar função

# Função formatar base loja
def base_loja():
    loja = pd.read_excel(r'C:\Users\mo_duarte\Desktop\RESOLVE\Indicadores\ResgateJa\BaseResgateJá\2021\01-2021\Resgate_Ja_Loja_21_01_2021.xlsx', header=2)
    def pos_mkt(loja):
      if loja['TIPO_DESPESA'] == 'RESGATE JA SAC':
          return 'Loja'
      else:
          return 'Restituição'
    loja['Canal'] = loja.apply(pos_mkt, axis=1)  # Função aplicada   
    return loja    

loja = base_loja() # Instanciar função

# Concatenar e formatar base
def base_resolve():
    base_tt = pd.concat([sac, loja]).fillna('0')
    base_tt.to_excel(r'C:\Users\mo_duarte\Desktop\RESOLVE\Indicadores\ResgateJa\BaseResgateJá\2021\01-2021\Base_Consolidada.xlsx')
    dinamica = pd.pivot_table(base_tt, columns='Canal', values='VALOR', aggfunc='sum')
    return print(dinamica)

# Resultado
base_resolve()