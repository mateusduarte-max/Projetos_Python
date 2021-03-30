import pandas as pd
import datetime as dt

def base_mkt():
    arquivo = pd.read_csv(r'C:\Users\mo_duarte\Desktop\RESOLVE\Indicadores\Painel Backlog\aba backlog 3p\tratativa-marketplace-view-2021-03-29-0954.csv', sep=',')
    arquivo['Solicitado'] = pd.to_datetime(arquivo['Solicitado'])
    arquivo['Hoje'] = dt.date.today()
    arquivo['Hoje'] = pd.to_datetime(arquivo['Hoje'])
    return arquivo

base = base_mkt()

def de_para_mkt():
    seller = pd.read_excel(r'C:\Users\mo_duarte\Desktop\RESOLVE\Indicadores\Painel Backlog\aba backlog 3p\De_para_Seller.xlsx')
    return seller

base_seller = de_para_mkt()

def base_merge():
    base_3 = base.merge(base_seller, how='left', on='Nome do Seller')
    base_4 = base_3.drop_duplicates(subset='ID', keep='last')
    def long_tail(base_4):
        if base_4['Classificação'] == 'Grande Varejo':
            return 'Grande Varejo'
        elif base_4['Classificação'] == 'Indústria':
            return 'Indústria'
        elif base_4['Classificação'] == 'Long Tail':
            return 'Long Tail'
        elif base_4['Classificação'] == '':
            return 'Long Tail'
        else:
            return 'Long Tail' 
    base_5 = base_4.copy() 
    base_5['Classificação_2'] = base_5.apply(long_tail, axis=1)   # Função aplicada   

    def atraso(base_5):
        if base_5['Solicitado'] < base_5['Hoje']:
            return 'Atrasado'
        else:
            return ''
    base_6 = base_5.copy()
    base_6['Atrasado'] = base_6.apply(atraso, axis=1)
    base_6.to_excel(r'C:\Users\mo_duarte\Desktop\RESOLVE\Indicadores\Painel Backlog\aba backlog 3p\teste.xlsx', index=False)
    return base_6

base_merge()