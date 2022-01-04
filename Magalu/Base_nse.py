import csv
from pymongo import MongoClient
import pandas as pd
from google.oauth2 import service_account


def get_gatewayDetails_freightType(result):
    ret = ""
    if result.get("gatewayDetails") != None:
        if result["gatewayDetails"].get("freightType") != None:
            ret = result["gatewayDetails"]["freightType"]      
    return ret
        
connection = 'mongodb://shippingUserAdminNew:yuv8Ir7OAyrdUj54g1@prd-inka-shipping-mongodb-01.netshoes.io,prd-inka-shipping-mongodb-02.netshoes.io,prd-inka-shipping-mongodb-03.netshoes.io/dbShippingBR?authSource=dbShippingBR&replicaSet=inka'
database = "dbShippingBR"
collection = "sellerGateway"

query = {}
client = MongoClient(connection)
db = client[database]
collection = db[collection]

totalRegistros = collection.count_documents(query)
print("Executando a query... Total: %s" %totalRegistros)

results = collection.find(query)
print("Exportando...")

with open('export-shipping-gateway.csv','w', newline='') as arquivo:
    writer = csv.writer(arquivo)
    writer.writerow(("Seller_ID", "Tipo_Gateway_Frete","Tipo_Frete_NSE"))
    count = 0
    for result in results:        
        writer.writerow((result["sellerId"],
                         result["gatewayName"],
                         get_gatewayDetails_freightType(result)
                         ))
        count +=1
        if count % 1000 == 0:
            print("%s exportados" %count)
            

sellers_nse = pd.read_csv(r'C:\Users\mo_duarte\Documents\GitHub\export-shipping-gateway.csv')
credentials = service_account.Credentials.from_service_account_file(r'C:\Users\mo_duarte\Desktop\Marketplace\chave_google\marketplace-analytics-333712-5416677751e5.json')
sellers_nse.to_gbq(destination_table='data.sellers_nets_nse',project_id='marketplace-analytics-333712', if_exists='replace', credentials=credentials, progress_bar=True)

print("Terminou!")