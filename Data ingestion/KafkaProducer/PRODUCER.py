#!/usr/bin/env python
# coding: utf-8

# In[1]:


from kafka import KafkaProducer
from datetime import datetime
import csv
import time
import json
import pytz
import configparser

#using configuration file to access data in them like files and table names
config = configparser.ConfigParser()
config.read('kafkaProducer.ini')
files = config['kafkaProducer']['files'].split(',')

def json_serializer(data):
    return json.dumps(data).encode(config['kafkaProducer']['encode'])

#using pytz library to get time in timezone as per snowflake account
timeZ_P = pytz.timezone(config['kafkaProducer']['timezone'])

#getting the topic name
topic=config['kafkaProducer']['topic']

#creating a producer instance to load data into kafka topics
producer= KafkaProducer(bootstrap_servers=[config['kafkaProducer']['bootstrap-servers']], value_serializer=json_serializer, )

#uploading the data files into kafka topics record by record(row by row) and adding timestamp and file identifier(name of table)
for i in range(0,len(files)):
    file = open(files[i], 'r')
    for line in file:
        producer.send(topic,line[0:len(line)-1]+','+str(datetime.now(timeZ_P).strftime('%Y-%m-%d %H:%M:%S.%f'))+','+config['kafkaProducer'][files[i]])
        print(line[0:len(line)-1]+','+str(datetime.now(timeZ_P).strftime('%Y-%m-%d %H:%M:%S.%f'))+','+config['kafkaProducer'][files[i]])
    time.sleep(0)


# In[ ]:




