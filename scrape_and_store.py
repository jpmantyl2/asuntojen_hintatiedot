# -*- coding: utf-8 -*-
"""
Created on Mon Sep 16 23:22:04 2019

@author: jpmantyl
"""

import requests
import pandas as pd
#import re
import sqlalchemy
from bs4 import BeautifulSoup

#test engine
engine = sqlalchemy.create_engine('postgresql://jpmantyl@:5432/jpmantyl')
print("engine tested")
postcodes = set(pd.read_json('/home/users/jpmantyl/asuntojen_hintatiedot/postcodes.json',dtype=str)['postcode'])

data = []
url_pre = r"https://asuntojen.hintatiedot.fi/haku/?search=1&l=0&c=&cr=1&pc=0&nc=0&h=1&h=2&h=3&r=1&r=2&r=3&r=4&amin=&amax=&submit=seuraava+sivu+%C2%BB&ps="

#for postcode in ['00200']:
for postcode in postcodes:

    data_postcode = []
    url_postcode = url_pre + str(postcode)
    
    z = -1
    while True:
        z = z + 1
        url = url_postcode + r"&z="+ str(z)
        response = requests.get(url)
        
        if(response.status_code == 200):
            sorsa = response.text#.split(sep='PRE>')
        else:
            print(f"Response status error.")
            print(f"Status: {response.status_code}")
            print(f"URL: {url}")
            raise(IOError)
            
        soup = BeautifulSoup(sorsa,features="html.parser")
        
        data_page = []
        blocks = soup.body.find_all('tbody')
        for block in blocks:
            rows = block.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                cols = [ele.text.strip() for ele in cols]
#                print(len(cols))
                if len(cols)==12:
                    pc = [postcode]
                    pc.extend([ele for ele in cols])
                    data_page.append(pc)
        if(len(data_page)):
            data_postcode.extend(data_page)
        else:
            break
    if(len(data_postcode)):
        data.extend(data_postcode)
    
    
column_names = ['postinumero'
                ,'kaupunginosa'
                ,'huoneisto'
                ,'talotyyppi'
                ,'neliot'
                ,'velaton_hinta'
                ,'neliohinta'
                ,'rakennusvuosi'
                ,'kerros'
                ,'hissi'
                ,'kunto'
                ,'tontti'
                ,'energialuokka']
df = pd.DataFrame(data,columns=column_names)

from datetime import datetime
df['load_dt'] = datetime.now()


df['neliot'] = pd.to_numeric(df['neliot'].str.replace(',','.'))
df['velaton_hinta'] = pd.to_numeric(df['velaton_hinta'])
df['neliohinta'] = pd.to_numeric(df['neliohinta'])
df['rakennusvuosi'] = pd.to_numeric(df['rakennusvuosi'])

#print(df['energialuokka'])
engine = sqlalchemy.create_engine('postgresql://jpmantyl@:5432/jpmantyl')
df.to_sql('asunnot_asuntojen_hintatiedot', engine, index=False, method='multi', if_exists='append')

