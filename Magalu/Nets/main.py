from connectionMongo import Connection
import openpyxl
import os.path
import time




date_ini = '2021-01-01'
today = time.strftime("%Y-%m-%d")

con = Connection()
result = con.find(date_ini, today)

path = 'OUTPUT/'
file_name = f'relatorio-nfe-{date_ini}-{today}.xlsx'

full_name = os.path.join(path, file_name)
new_xlsx = openpyxl.Workbook()
print(f"Create workbook {full_name}:")
sheet = new_xlsx.active
sheet.title = 'Relatorio'
sheet = new_xlsx['Relatorio']

line = 2

sheet['A1'] = 'Pedido externo'
sheet['B1'] = 'SellerID'
sheet['C1'] = 'Nome seller'
sheet['D1'] = 'CNPJ'
sheet['E1'] = 'Mensagem de erro'

for i in result:
    seller = i['seller']
    sheet[f'A{line}'] = i['_id']['orderNumber']
    sheet[f'B{line}'] = seller['code']
    sheet[f'C{line}'] = seller['name']
    sheet[f'D{line}'] = seller['supplierCnpj']
    sheet[f'E{line}'] = str(i['shippings'])
    line = line + 1

new_xlsx.save(full_name)
print(f'Found {line-2} records')
print('Done ...')
