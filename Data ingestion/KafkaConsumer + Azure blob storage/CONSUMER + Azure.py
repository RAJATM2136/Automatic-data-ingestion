#!/usr/bin/env python
# coding: utf-8

# In[1]:


from kafka import KafkaConsumer
from json import loads
import json
import os, uuid
import csv
import pytz
import time
from datetime import datetime
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from cryptography.fernet import Fernet
import configparser

#importing config files
config = configparser.ConfigParser()
config.read('KafkaConsumerToAzureBlobs.ini')

#encoding key used for encryption of passwords and other key information 
fernet_key='VlUVeecxDDQTIsdD3fK4J1yuNr0QsPO0kBGU-6yEZxQ='.encode()

def config_decrypt(encoded):
    key = fernet_key
    fernet = Fernet(key)
    encoded=encoded[2:len(encoded)-1]
    return fernet.decrypt(encoded.encode()).decode()


#getting BlobServiceCLient and primary key from config files
key = config_decrypt(config['azure']['key'])
blob_service_client = BlobServiceClient.from_connection_string(key)

# getting information of tables and their mapping to each container
tables = config['azure']['tables'].split(',')
table_container_dict={}
for i in range(0,len(tables)):
    table_container_dict[tables[i]]=config['azure'][tables[i]]


#getting topic and creating a consumer instance
topic=config['kafka']['topic']
consumer = KafkaConsumer(topic, bootstrap_servers=[config['kafka']['bootstrap-servers']], auto_offset_reset='earliest', enable_auto_commit=False
                   )

timeZ_P = pytz.timezone(config['kafka']['timezone'])
print(datetime.now(timeZ_P).strftime('%H:%M:%S'))

print(topic)
records=0
#starting the consumer
for message in consumer:
    container_file_name = str(uuid.uuid4()) + ".csv"
    s=message.value
    d=s.decode('utf-8')
    d=d.replace("\"","")
    d=d.replace(", ,",",,")
    s_len=len(d)
    index=d.rfind(',')
    container_name=table_container_dict[d[index+1:s_len]]
    records=records+1
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=container_file_name)
    blob_client.upload_blob(d)
    print(d)
    print(records)


# In[ ]:




