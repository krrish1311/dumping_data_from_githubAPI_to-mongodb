from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import os
import pymongo
from dotenv import load_dotenv
load_dotenv()
headers={'Authorization':'Bearer '+os.getenv('GITHUB_AUTH_KEY') }
def data_cleaning(repo_info):
    repositories=[]
    for i in range(len(repo_info)):
        temp={}
        for j in repo_info[i]:
#             print(j,'_url' not in j,j!='owner')
            if '_url' not in j and j!='owner':
                temp[j]=repo_info[i][j]
        repositories.append(temp)

    return repositories
    # 
def fetching_data():
        try:
            response=requests.get(url=os.getenv('URL'),headers=headers )
            if response.status_code==200:
                cleaned_data=data_cleaning(response.json())
            else:
                print('The Github API is not responding')
        except Exception as e:
            print('the error is ',e)
        return cleaned_data
        # return {'first':'hii','second':'bye'}

def dumping_data(**context):
    repositories=context['task_instance'].xcom_pull(task_ids='task01')
    try:
        
        print('the values are :',repositories)
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client["my_github"]
        col = db["all_repos2"]
        col.insert_many(repositories)
    except Exception as e:
        print("THE errors are",e)
    finally:
        client.close()

default_args = {
    'owner': 'krrish',
    'start_date': datetime(2023,4,12),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='fetching_git',
    default_args=default_args,
    description='My third DAG',
    schedule_interval=timedelta(days=1),
) as dag:
    task1=PythonOperator(
        task_id='task01',
        python_callable=fetching_data,
        provide_context=True
        
    )
    task2=PythonOperator(
        task_id='task02',
        python_callable=dumping_data,
        provide_context=True
    )
    task1>>task2        