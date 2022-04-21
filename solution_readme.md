Author: Debi prasad mishra
Language: python3
IDE: jupyter

filename: pipeline.py

api call filename: app_api.py

HOW TO DEPLOY:
I am unable to upload python environment tar file as its size was more than 100MB. So you need to create python virtual environment using below steps

1: Python 3.8.10 should be installed in the linux system

2: pip3 install virtualenv

3: create virtual environment (virtualenv sephora_test) and activate
 
4: run "pip install -r requirements.txt"

5: FOR LOCAL RUN  python pipeline.py --res_dir '/opt/sephora/data-test-sde-master/res/' --log_dir '/opt/sephora/' --project_name 'sephora-test-347812' --key_path '/opt/sephora/sephora-test-347812-5fd519c10f6d.json' --tasks_json '/opt/sephora/tasks.json'

6: For API CALL RUN: (i)python app_api.py (ii) curl -X POST 'http://10.1.5.121:8181/run'

I have attched screenshots of successful runs. 

APPROACH:

As per my understanding, This assignment tests mainly 3 things:

1- Execute chain of tasks with correct dependency order, execute parallel tasks in the same group/level

2- Familiarity with bigquery database and API

3- API implementation

USER INPUTS IN THE CODE:

res_dir : absolute path to res directory (example: '/opt/sephora/data-test-sde-master/res/')

log_dir : absolute path to log directory (example: '/opt/sephora/')

project_name : bigquery project name ('sephora-test-347812')

key_path : absolute path to bigquery access keyfile ('/opt/sephora/sephora-test-347812-5fd519c10f6d.json')

tasks_json : tasks file with dependencies ('/opt/sephora/tasks.json')

CREATE A JSON FILE OF TASKS AND THEIR DEPENDENCIES LIKE BELOW

{"tmp/variants.sql": ["tmp/item_purchase_prices.sql","tmp/variant_images.sql","tmp/inventory_items.sql"],
    "tmp/products.sql":["tmp/product_images.sql","tmp/product_categories.sql"],
    "tmp/variants.sql":["tmp/item_purchase_prices.sql","tmp/variant_images.sql","tmp/inventory_items.sql"],
    "final/products.sql":["tmp/products.sql","tmp/variants.sql"]}

Then save it as a json file (example: tasks.json)	

BRIEF DESCRIPTION OF THE CODE:

1: Read user inputs. Parse them.

2: Create Dictionary from json file of tasks. Throw exception if invalid json file received. Stop further execution

3: Create DAG. Throw exception if DAG is invalid(DAG has cycle). Stop further execution 

4: Create group of parallel tasks in the DAG

5: Execute list of parallel tasks in parallel using multiprocessing. The number of threads which is same as the number of parallel tasks will be created
   Throw exception if a task fails. Stop further execution

Detail of Step 5:

i- parse the sql script

ii- invoke big query api to execute parsed sql query and save it to dataframe

iii- extract dataset and table name

iv- again call bigquery api to save the table (overwrite)
 
API CALL:

1- Flask app 

2: run app_api.py

3: Open another terminal and run curl -X POST 'http://10.1.5.121:8181/run'

