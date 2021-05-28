#!/usr/bin/env python
# coding: utf-8

# In[1]:


def CreatingBucket(client,name):
    # Set properties on a plain resource object.
    bucket = storage.Bucket(client=client,name=name)
    bucket.location = "asia-south1"
    bucket.storage_class = "STANDARD"
    # Pass that resource object to the client.
    bucket = client.create_bucket(bucket)
    return bucket


# In[2]:


def connections():
    bqclient = bigquery.Client()
    store_client=storage.Client()
    return bqclient,store_client


# In[3]:


def plotting(para1,para2):
    plt=sns.barplot((data2.groupby(para1)[para2].sum().sort_values(ascending=False).head(10)),(data2.groupby(para1)[para2].sum().sort_values(ascending=False).head(10)).index)
    fig=plt.get_figure()
    fig.savefig(para1+para2+'.png')


# In[4]:


import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import bigquery_storage
import time
import sys
import warnings
from google.api_core import exceptions as exceptionss
import matplotlib.pyplot as plt
import seaborn as sns

import os
warnings.filterwarnings('ignore')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=r"C:\Users\Deepak Agrawal\Downloads\effective-forge-313405-369ac9d9c5b1.json"
try:
    bqclient,store_client = connections()
except:
    print("Cannot establish connection to storage and bigquery client")
    exit(0)
    
try:
    QUERY = (
        'SELECT * FROM `bigquery-samples.reddit.words_unfiltered` LIMIT 20000')
    query_job = bqclient.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
except exceptionss.NotFound as e:
    print("Table not found")
    exit(0)
try:
    if rows.total_rows==0:
        print("No rows found Please check")
        exit(0)
    else:
        pass
except:
    pass
    
data=pd.DataFrame(rows.to_dataframe())

name='draster-bucket4'
try:
    bucket=CreatingBucket(store_client,name)
    print("New Bucket Created: {}".format(bucket.name))
except:
    print(" {} Bucket Already exists.........".format(name))
    print("Deleting the existing Bucket..........")
    bucket=store_client.get_bucket(name)
    bucket.delete(force=True)
    print("Bucket Deleted \n")
    time.sleep(60)
    print("Creating a new Bucket...........")
    bucket=CreatingBucket(store_client,name)
    print("New Bucket Created: {}".format(bucket.name))
data.to_csv("Reddit_words.csv")
try:
    blob=bucket.blob('Reddit_words.csv')
    with open("Reddit_words.csv","rb") as r:
        blob.upload_from_file(r)
except Exception as e:
    print(e)
    exit(0)
try:
    bqclient.create_dataset("Reddit")
except:
    print("dataset already exists")
    pass
table_name="effective-forge-313405.Reddit.words_unfiltered"
try:
    table=bqclient.create_table("effective-forge-313405.Reddit.words_unfiltered")
except Exception as e:
    if 'Already Exists' in e.args[0]:
        print("Table Already Exits")
        pass
    else:
        print(e)
        exit(0)
time.sleep(10)    
job_config = bigquery.LoadJobConfig(source_format='CSV')
job_config.autodetect=True
job_config.source_format=bigquery.SourceFormat.CSV
bigqueryJob = bqclient.load_table_from_uri("gs://draster-bucket4/Reddit_words.csv", table_name, job_config=job_config)
bigqueryJob.result()

try:
    blob=bucket.get_blob("Reddit_words.csv")
    from io import StringIO
    st=blob.download_as_string()
    data1=pd.read_csv(StringIO(str(st,"utf-8")))
except Exception as e:
    print(e)
    exit(0)

data1=data1.drop(columns=['Unnamed: 0'],axis=1)
data2=data1.sort_values('total',ascending=False)
#(data2.groupby('title')['total'].sum().sort_values(ascending=False).head(10))

plotting('title','total')
#plt1=sns.barplot((data2.groupby('title')['total'].sum().sort_values(ascending=False).head(10)),(data2.groupby('title')['total'].sum().sort_values(ascending=False).head(10)).index)
plotting('title','ups')
#plt2=sns.barplot((data2.groupby('title')['ups'].sum().sort_values(ascending=False).head(10)),(data2.groupby('title')['ups'].sum().sort_values(ascending=False).head(10)).index)
plotting('title','downs')
#plt3=sns.barplot((data2.groupby('title')['downs'].sum().sort_values(ascending=False).head(10)),(data2.groupby('title')['downs'].sum().sort_values(ascending=False).head(10)).index)
plotting('title','num_comments')
#plt5=sns.barplot((data2.groupby('title')['num_comments'].sum().sort_values(ascending=False).head(10)),(data2.groupby('title')['num_comments'].sum().sort_values(ascending=False).head(10)).index)
plotting('subr','num_comments')
#plt6=sns.barplot((data2.groupby('subr')['num_comments'].sum().sort_values(ascending=False).head(10)),(data2.groupby('subr')['num_comments'].sum().sort_values(ascending=False).head(10)).index)
plt4=sns.barplot(data2[data2.ups<data2.downs]['title'],data2[data2.ups<data2.downs]['downs'])

for i in os.listdir():
    if 'png' in i:
        blob=bucket.blob(i)
        blob.content_type='image'
        with open(i,'rb') as f:
             blob.upload_from_file(f)


# In[ ]:





# In[ ]:




