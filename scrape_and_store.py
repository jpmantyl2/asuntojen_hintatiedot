# -*- coding: utf-8 -*-
"""
Created on Mon Sep 16 23:22:04 2019

@author: jpmantyl
"""

import requests
import pandas as pd
import sqlalchemy
from bs4 import BeautifulSoup
import os
from datetime import datetime

results_database_connstr = os.environ['CONN_STR']
postcodes_file_path = os.environ['POSTCODES_FILE']

#Test database engine before scraping.
engine = sqlalchemy.create_engine(results_database_connstr)
print("engine tested")

postcodes = set(pd.read_json(postcodes_file_path,dtype=str)['postcode'])

data = []
url_pre = r"https://asuntojen.hintatiedot.fi/haku/?search=1&l=0&c=&cr=1&pc=0&nc=0&h=1&h=2&h=3&r=1&r=2&r=3&r=4&amin=&amax=&submit=seuraava+sivu+%C2%BB&ps="

#for postcode in ['00200','02200']: # just debugging if needed
for postcode in postcodes:

    data_postcode = []
    url_postcode = url_pre + str(postcode)
    
    # index z is used for traversing the subpages of each postcode url
    z = -1
    while True:
        z = z + 1
        url = url_postcode + r"&z="+ str(z)
        response = requests.get(url)
        
        if(response.status_code == 200):
            sorsa = response.text
        else:
            print(f"Response status error.")
            print(f"Status: {response.status_code}")
            print(f"URL: {url}")
            raise(IOError)
            
        soup = BeautifulSoup(sorsa,features="html.parser")
        
        data_page = []
        # Navigate using html tags and extract relevant data
        # Relevant data is known to exist in one of the tbody elements in an html table.
        # Traverse each tbody in search for tags tr and td.
        blocks = soup.body.find_all('tbody')
        for block in blocks:
            rows = block.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                cols = [ele.text.strip() for ele in cols]
                # Valid data rows have exactly 12 columns.
                if len(cols)==12:
                    pc = [postcode]
                    pc.extend([ele for ele in cols])
                    data_page.append(pc)
        if(len(data_page) > 0):
            data_postcode.extend(data_page)
        else:
            break
    if(len(data_postcode) > 0):
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

df['load_dt'] = datetime.now()

# data type conversions
df['neliot'] = pd.to_numeric(df['neliot'].str.replace(',','.'))
df['velaton_hinta'] = pd.to_numeric(df['velaton_hinta'])
df['neliohinta'] = pd.to_numeric(df['neliohinta'])
df['rakennusvuosi'] = pd.to_numeric(df['rakennusvuosi'])

# Append df into database
df.to_sql('asunnot_asuntojen_hintatiedot', engine, index=False, method='multi', if_exists='append')

# Print information from which the successful code execution can be verified.
# An email containing printed information is automatically sent to owner in Kapsi environment when this code is run with crontab :)
print(len(df))

sql = 'select load_dt, count(*) from asunnot_asuntojen_hintatiedot group by load_dt order by load_dt desc'
with engine.connect() as con:
    rs = con.execute(sql)
    for row in rs:
        print(row)
