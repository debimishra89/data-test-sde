#!/usr/bin/env python
# coding: utf-8

# In[4]:


import networkx as nx
from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
from google.cloud import bigquery
import requests
import flask
import os
import sys
from multiprocessing import Pool
import logging
import argparse
import warnings
import json
warnings.filterwarnings('ignore')


# In[5]:


def createDAG(dict_dependency):
    list_nodes_sql=[]
    for i in dict_dependency:
        list_nodes_sql.extend([(x,i) for x in dict_dependency[i]])
    graph= nx.DiGraph()
    graph.add_edges_from(list_nodes_sql)
    if(nx.is_directed_acyclic_graph(graph)):
        logging.info('DAG creation successful')
        return graph
    else:
        logging.error('Dependencies has a cycle. Cant create DAG')
        sys.exit("Dependencies has a cycle. Can't create DAG. Job execution stopped")
            
#TASK GROUPING FOR PARALLEL EXECUTION
def topsort_grouping(g):
    # copy the graph
    _g = g.copy()
    res = []
    # while _g is not empty
    while _g:
        zero_indegree = [v for v, d in _g.in_degree() if d == 0]
        res.append(zero_indegree)
        _g.remove_nodes_from(zero_indegree)
    logging.info('DAG with parallel tasks created')
    return res

    #TASK EXECUTION ON BIGQUERY DATASETS
def execute(task):
    logging.info('Task execution started for {}'.format(task))
    sql_query = open(task, 'r')
    df = client.query(sql_query.read()).to_dataframe()
    #DATASET and TABLE NAME EXTRACTION
    t1=task.split('/')
    t2=t1[1].split('.')[0]
    dataset_table=t1[0]+'.'+t2
    table_id = project_name+'.'+dataset_table
    #DATA LOADING TO BIGQUERY TABLES
    job_config = bigquery.LoadJobConfig(autodetect=True,write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(
    df, table_id, job_config=job_config)  # Make an API request.
    job.result()
    logging.info('Task execution ended for {}'.format(task))
   


# In[6]:


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--res_dir')
    parser.add_argument('--log_dir')
    parser.add_argument('--project_name')
    parser.add_argument('--key_path')
    parser.add_argument('--tasks_json')
    args = parser.parse_args()
    
    
    os.chdir(args.res_dir) #'/opt/sephora/data-test-sde-master/res'
    log_dir=args.log_dir #'/opt/sephora/'
    project_name=args.project_name #'sephora-test-347812'
    client = bigquery.Client.from_service_account_json(args.key_path) #"/opt/sephora/sephora-test-347812-5fd519c10f6d.json"
    
    #Logging setup
    logging.basicConfig(filename=log_dir+project_name+'.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)
    
    #USER INPUT TASKS
    try:
        with open(args.tasks_json) as json_file:
            dict_dependency = json.load(json_file)
    except Exception as e:
        logging.error("Error occured while decoding tasks json file")
        sys.exit("problem with input tasks file, job execution stopped")

    logging.info('input tasks received')
    
    #DAG CREATION
    logging.info('DAG creation process started')
    graph=createDAG(dict_dependency)
    
    #PARALLEL TASKS LIST
    logging.info('Grouping of parallel tasks process started')
    tasks=topsort_grouping(graph)
    for i in range(len(tasks)):
        logging.info('List of parallel tasks({}) :{}'.format(i+1,tasks[i]))

    #PARALLEL EXECUTION OF DAG TASKS
    for task in tasks:
        try:
            with Pool(len(task)) as p:
                p.map(execute,task)
        except Exception as e:
            logging.error('Error occured while executing : %s',task)
            logging.error('Error message: %s',e)
            sys.exit("stopped execution")
            
        
    
    


# In[ ]:




