# import the required airflow modules
import airflow
from airflow import DAG
from airflow.utils.dates import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from includes.get_pictures import _get_pictures
from airflow.models import Variable

import smtpd
import ssl
from email.message import EmailMessage 

email_sender = "nnamanichisomnc@gmail.com"
email_password = ""
email_receiver =

def task_fail_alert(context):
    state = context.get("task_instance").state
    dag = context.get("task_instance").dag_id
    task = context.get("task_instance").task_id
    exec_date = context.get("task_instance").start_date
    log = context.get("task_instance").log_url
    env_name = context["params"].get("environment")
    dag_owner = context["param"].get("dag_owner")

    subject = f"task {task} in dag {dag} failed"
    body = f'''
    Hey {dag_owner}

    The task {task} in dag {dag} running in {env_name} has failed for run date {exec_date}.

    Here is the log url: {log}
    '''

    em = EmailMessage()
    em['From'] = email_sender
    em['To'] = email_reciever
    em['Subject'] = subject
    em.set_content(body)

# Create default arguments
default_args = {
    "on_failure_callbac": task_fail_alert,
    "params": {
        "environment": Variable.get("environment"),
        "dag_owner": "Chisom"
    }
}

# Instantitae a DAG - First Method
# dag = DAG(
#     dag_id="rocket",
#     start_date=datetime(2025, 11, 18),
#     schedule_interval=None  
# )

# Instatiate a DAG - Second Method
# This is using a context manager to manage your resources
with DAG(
    dag_id="rocket",
    start_date=datetime(2024, 11, 27),
    schedule_interval=None,
    catchup=False,
    tags=['CoreDataEngineer'],
    default_args=default_args
) as dag:
    
    # the first task: configure a bash operator to download launches
    download_launches = BashOperator(
        task_id='download_launches',
        bash_command="curl -o /opt/airflow/dags/rockets/launches/launches.json -L https://ll.thespacedevs.com/2.0.0/launch/upcoming/"
    )
        
 
    # the second task: write a function to download all pictures in the launches.json file
    get_pictures = PythonOperator(
        task_id='get_pictures',
        python_callable=_get_pictures
    )

    # the third task: create a notification task that runs bash echo command
    notify = BashOperator(
        task_id='notify',
        bash_command='echo "There are now $(ls /opt/airflow/dags/rockets/images/ | wc -1) images"'
    )

    # specify the dependencies
    download_launches >> get_pictures >> notify
