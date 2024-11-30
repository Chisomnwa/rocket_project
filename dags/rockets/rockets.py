# import the required airflow modules
import airflow
from airflow import DAG
from airflow.utils.dates import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from rockets.includes.get_pictures import _get_pictures
from rockets.includes.email_sender import task_fail_alert
from airflow.models import Variable

# Create default arguments
default_args = {
    "on_failure_callback": task_fail_alert,
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
