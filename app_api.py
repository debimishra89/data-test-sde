#!/usr/bin/env python
# coding: utf-8

# In[17]:


from flask import Flask, redirect, url_for, request
from google.cloud import bigquery
import requests
from flask import Flask, request
import os
import sys
import json
from multiprocessing import Pool
import logging
import warnings
warnings.filterwarnings('ignore')


# In[18]:


import pipeline

# create the Flask app
app = Flask(__name__)

@app.route("/run",methods=['POST'])
def query_example():
    os.chdir('/opt/sephora/data-test-sde-master/res')
    log_dir='/opt/sephora/'
    pipeline.project_name='sephora-test-347812'
    pipeline.client = bigquery.Client.from_service_account_json("/opt/sephora/sephora-test-347812-5fd519c10f6d.json")
    tasks_json='/opt/sephora/tasks.json'
    #Logging setup
    logging.basicConfig(filename=log_dir+pipeline.project_name+'.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
    logging.info("API call initiated")
    #USER INPUT TASKS
    try:
        with open(tasks_json) as json_file:
            dict_dependency = json.load(json_file)
    except Exception as e:
        logging.error("Error occured while decoding tasks json file")
        sys.exit("problem with input tasks file, job execution stopped")

    graph=pipeline.createDAG(dict_dependency)
    tasks=pipeline.topsort_grouping(graph)
    
    for i in range(len(tasks)):
        logging.info('List of parallel tasks({}) :{}'.format(i+1,tasks[i]))


    for task in tasks:
        try:
            with Pool(len(task)) as p:
                p.map(pipeline.execute,task)
        except Exception as e:
            sys.exit('Error message: %s',e)
        
                
    return "success"


if __name__ == '__main__':
    # run app in debug mode on port 5000
    app.run(host='0.0.0.0', port=8181) #debug=True, port=5050


# In[ ]:





# In[ ]:




