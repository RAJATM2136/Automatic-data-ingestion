#!/usr/bin/env python
# coding: utf-8

# In[1]:


import snowflake.connector
import os
from datetime import datetime
import time
import pytz
import pandas as pd
from multiset import Multiset
import matplotlib.pyplot as plt
from matplotlib_venn import venn2,venn2_circles
from matplotlib_venn import venn3, venn3_circles
import configparser
from cryptography.fernet import Fernet
from kafka import KafkaConsumer

#getting the required configuration files
fernet_key='VlUVeecxDDQTIsdD3fK4J1yuNr0QsPO0kBGU-6yEZxQ='.encode()

config = configparser.ConfigParser()
config.read('data recon.ini')

def config_decrypt(encoded):
    key = fernet_key
    fernet = Fernet(key)
    encoded=encoded[2:len(encoded)-1]
    return fernet.decrypt(encoded.encode()).decode()

#using snowflake connector to connect to snowflake account
con = snowflake.connector.connect(
    user=config['snowflake']['user'],
    password=config_decrypt(config['snowflake']['password']),
    account=config['snowflake']['account']
)

# getting snowflake cursor object 
cs = con.cursor()

#executing sql commands to use snowflake in givn role,database and warehouse
cs.execute('USE warehouse '+config['snowflake']['warehouse'])
cs.execute('USE database '+config['snowflake']['database'])
cs.execute('USE ROLE '+config['snowflake']['role'])

#getting tables
tables = config['data recon']['tables']
tables = tables.split(',')

#getting stages as mapped to tables
stages = []
for i in  range(0,len(tables)):
    stages.append(config['map_stage_table'][tables[i]])
stages

#getting timestamp

timeZ_P = pytz.timezone(config['kafka']['timezone'])

#getting time upper limit for data

date_time=""

if(config['data recon']['time']!=''):
    date_time = config['data recon']['time']
    curr_datetime = datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S.%f')
else:
    date_time = datetime.now(timeZ_P).strftime('%Y-%m-%d %H:%M:%S.%f')
    curr_datetime = datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S.%f')

#sql generation
tables_sql={}
stages_sql={}

for i in range(0,len(tables)):
    table = tables[i]
    stage = stages[i]
    columns = int(config['number_of_columns'][table])-1
    timestamp_column = int(config['timestamp_column'][table])#timestamp added by producer
    sql1 = 'select '
    for j in range(1,columns+1):
        sql1 = sql1+'$'+str(j)+'::VARCHAR'
        if(j!=columns):
            sql1 = sql1+','
    sql1 = sql1+' from '
    sql2 = ' where (($'+str(timestamp_column)+' is null)or(TO_TIMESTAMP_NTZ($'+str(timestamp_column)+')<TO_TIMESTAMP_NTZ('+'\''+date_time+'\')))'
    stage_sql = sql1+'@'+stage+sql2
    table_sql = sql1+table+sql2
    stages_sql[stage] = stage_sql
    tables_sql[table] = table_sql
    

    
#getting data from kafka topic
topic = config['kafka']['topic']
#creating a consumer instance
consumer = KafkaConsumer(topic, bootstrap_servers=[config['kafka']['bootstrap-servers']], auto_offset_reset='earliest', enable_auto_commit=False, consumer_timeout_ms=1000
                   )

df = pd.DataFrame()
df_kafka={}
for i in range(0,len(tables)):
    df_kafka[tables[i]]=df

for message in consumer:
    s=message.value
    d=s.decode('utf-8')
    d=d[1:len(d)-1]
    df = pd.DataFrame()
    l=d.split(",")
    table_name = l[len(l)-1]
    record_datetime = datetime.strptime(l[len(l)-2], '%Y-%m-%d %H:%M:%S.%f')
    if(record_datetime>=curr_datetime):
        break
    l=l[0:len(l)-1] # this removes table_naame
    f=pd.Series(data=l)
    df=df.append(f,ignore_index=True)
    df_kafka[table_name]=df_kafka[table_name].append(df)

df_stages = {}
for i in range(0,len(stages)):
    sql = stages_sql[stages[i]]
    cs.execute(sql)
    df_stages[stages[i]]=cs.fetch_pandas_all()
    
#snowflake dataframes and getting data from snowflake tables
df_snowflake = {}
for i in range(0,len(tables)):
    sql= tables_sql[tables[i]]
    cs.execute(sql)
    df_snowflake[tables[i]]=cs.fetch_pandas_all()
    
    
    
dataset={}
for i in range(0,len(tables)):
    group={}
    dataset[tables[i]]=group
    
# storing all collected data in dataset
#dataset[<table_name>][azure] will show rows in external stage for that table
#dataset[<table_name>][snowflake] will show rows in snowflake table
#dataset[<table_name>][kafka] will show rows in topic for that table
for i in range(0,len(tables)):
    dataset[tables[i]]['kafka'] = df_kafka[tables[i]]
    dataset[tables[i]]['azure'] = df_stages[stages[i]]
    dataset[tables[i]]['snowflake'] = df_snowflake[tables[i]]

datarecon={}
for i in range(0,len(tables)):
    group={}
    datarecon[tables[i]]=group

    
#datarecon is a dictionary of tables
#each datarecon[<table_name>] is a dictionary of pandas dataframe
#idea is based on set theory
#therefore 
# [kafka-azure] will show missing files that didnt reach azure stage
# [azure-kafka] shows additional data in azure which isn't from kafka
# [azure-snoflake] shows all files that reached azure stages but didnt make it to snowflake
# [snowflake-azure] shows all files that reached snowflake from sources other than azure
# [kafka-snowflake] shows files that didnt reach from kafka to snowflake
# [snowflake-kafka] shows files in snowflake present from sources other than kafka
# datarecon[<table_name>][kafka-azure] shows files that didnt reach azure from kafka

for i in range(0,len(tables)):
    ds1 = Multiset([tuple(line) for line in df_kafka[tables[i]].values])
    ds2 = Multiset([tuple(line) for line in df_stages[stages[i]].values])
    ds3 = Multiset([tuple(line) for line in df_snowflake[tables[i]].values])
    datarecon[tables[i]]['kafka-azure'] = pd.DataFrame(list(ds1.difference(ds2)))
    datarecon[tables[i]]['azure-kafka'] = pd.DataFrame(list(ds2.difference(ds1)))
    datarecon[tables[i]]['azure-snowflake'] = pd.DataFrame(list(ds2.difference(ds3)))
    datarecon[tables[i]]['snowflake-azure'] = pd.DataFrame(list(ds3.difference(ds2)))
    datarecon[tables[i]]['kafka-snowflake'] = pd.DataFrame(list(ds1.difference(ds3)))
    datarecon[tables[i]]['snowflake-kafka'] = pd.DataFrame(list(ds3.difference(ds1)))
    
# example of accessing datarecon
datarecon['COUNTRY_WISE_LATEST']['azure-kafka']
# example of acessing dataset
dataset['COUNTRY_WISE_LATEST']['kafka']
dataset['COUNTRY_WISE_LATEST']['azure']
    
# function used for pictorial representation

def venn2circles(i):
    table=tables[i]
    print('data reconciliation for '+tables[i])
    print(datarecon[tables[i]]['kafka-azure'].shape[0],dataset[tables[i]]['kafka'].shape[0]-datarecon[tables[i]]['kafka-azure'].shape[0],datarecon[tables[i]]['azure-kafka'].shape[0])
    venn2(subsets = (datarecon[tables[i]]['kafka-azure'].shape[0],datarecon[tables[i]]['azure-kafka'].shape[0],dataset[tables[i]]['kafka'].shape[0]-datarecon[tables[i]]['kafka-azure'].shape[0]), set_labels = ('kafka topic', 'azure stage'))
    venn2_circles(subsets=(datarecon[tables[i]]['kafka-azure'].shape[0],datarecon[tables[i]]['azure-kafka'].shape[0],dataset[tables[i]]['kafka'].shape[0]-datarecon[tables[i]]['kafka-azure'].shape[0]),linestyle='dotted') 
    plt.show()
    
    print(datarecon[tables[i]]['azure-snowflake'].shape[0],dataset[tables[i]]['azure'].shape[0]-datarecon[tables[i]]['azure-snowflake'].shape[0],datarecon[tables[i]]['snowflake-azure'].shape[0])
    venn2(subsets = (datarecon[tables[i]]['azure-snowflake'].shape[0],datarecon[tables[i]]['snowflake-azure'].shape[0],dataset[tables[i]]['azure'].shape[0]-datarecon[tables[i]]['azure-snowflake'].shape[0]), set_labels = ('azure stage', 'snowflake'))
    venn2_circles(subsets=(datarecon[tables[i]]['azure-snowflake'].shape[0],datarecon[tables[i]]['snowflake-azure'].shape[0],dataset[tables[i]]['azure'].shape[0]-datarecon[tables[i]]['azure-snowflake'].shape[0]),linestyle='dotted') 
    plt.show()
    
    print(datarecon[tables[i]]['kafka-snowflake'].shape[0],dataset[tables[i]]['snowflake'].shape[0]-datarecon[tables[i]]['snowflake-kafka'].shape[0],datarecon[tables[i]]['snowflake-kafka'].shape[0])
    venn2(subsets = (datarecon[tables[i]]['kafka-snowflake'].shape[0],datarecon[tables[i]]['snowflake-kafka'].shape[0],dataset[tables[i]]['snowflake'].shape[0]-datarecon[tables[i]]['snowflake-kafka'].shape[0]), set_labels = ('kafka', 'snowflake'))
    venn2_circles(subsets=(datarecon[tables[i]]['kafka-snowflake'].shape[0],datarecon[tables[i]]['snowflake-kafka'].shape[0],dataset[tables[i]]['snowflake'].shape[0]-datarecon[tables[i]]['snowflake-kafka'].shape[0]),linestyle='dotted') 
    plt.show()
    
    print('rows in kafka',dataset[tables[i]]['kafka'].shape[0])
    print('rows in azure',dataset[tables[i]]['azure'].shape[0])
    print('rows in snowflake',dataset[tables[i]]['snowflake'].shape[0])
    print('rows that are present in kafka and not in azure :', datarecon[tables[i]]['kafka-azure'].shape[0])
    print('rows that are additional azure :', datarecon[tables[i]]['azure-kafka'].shape[0])
    print('rows that are present in azure and not in snowflake', datarecon[tables[i]]['azure-snowflake'].shape[0])
    print('additonal rows present in snowflake and not in azure', datarecon[tables[i]]['snowflake-azure'].shape[0])
    print('rows that are present in kafka and not in snowflake table: ', datarecon[tables[i]]['kafka-snowflake'].shape[0])
    print('additional rows that are present in snowflake and not in kafka: ', datarecon[tables[i]]['snowflake-kafka'].shape[0])
    
    
# visual venn representation of each table for missing and additional files

for i in range(0,len(tables)):
    venn2circles(i)
    print('\n')
    print('*******************************************************************')
    print('\n')


# In[ ]:




