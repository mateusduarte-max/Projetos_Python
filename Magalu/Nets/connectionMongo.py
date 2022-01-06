from pymongo import MongoClient
import datetime

class Connection():
    def __init__(self):
        client = MongoClient('mongodb://USUARIO:SENHA@az-br-prd-mp-order-mongodb-01.netshoes.io:27017/mp-order?authSource=mp-order')
        db=client['mp-order']
        self.collection=db['sellerOrder']

    def find(self, date_ini, date_fin):
        
        result=self.collection.find({
                                        "shippings.invoice.status": "ERROR",
                                        'orderDate': {
                                            "$gte": datetime.datetime(2022,1,1),
                                            "$lte": datetime.datetime(2022,1,30)
                                            }
                                        }, {
                                         "_id.orderNumber": 1,
                                         "seller.code": 1,
                                         "seller.name": 1,
                                         "seller.supplierCnpj": 1,
                                         "shippings.invoice.errors.message": 1
                                        })
        return result
    
    def findByCNPJ(self, cnpj):
    
        result=self.collection.find({
                                    "shippings.invoice.status": "ERROR",
                                    "seller.supplierCnpj":cnpj
                                    }, {
                                     "_id.orderNumber": 1,
                                     "seller.code": 1,
                                     "seller.name": 1,
                                     "seller.supplierCnpj": 1,
                                     "shippings.invoice.errors.message": 1
                                    })
        return result