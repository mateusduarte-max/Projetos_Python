import pandas as pd
from pandas import json_normalize
from sqlalchemy import create_engine
import requests as r
import json

desk = 'donatelo.freshdesk.com'
desk_URL = 'https://' + desk
auth_deets = ('AYeCkCcm3yTLwvcEqc', 'x')
cat_query = r.get(desk_URL + '/api/v2/search/tickets?query="custom_string: finance"', 
                  auth = auth_deets).content



df = pd.read_json(cat_query)

'''campos_personalizados = json_normalize(df)
print(campos_personalizados.info())'''

df.head()