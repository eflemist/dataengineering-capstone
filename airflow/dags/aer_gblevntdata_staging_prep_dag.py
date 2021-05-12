#----------------
# Standard library imports
#----------------
from datetime import datetime

import logging
import os
import os.path
import sys

absFilePath = os.path.abspath(__file__)
fileDir = os.path.dirname(os.path.abspath(__file__))
parentDir = os.path.dirname(fileDir)

sys.path.append(parentDir)

#----------------
# Third Party imports
#----------------

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

#---------------
# Local app imports
#--------------

import gblevent_config as gblevent_config

#--------------
# variable setup
#--------------

sshHook = SSHHook('aws_emr')

spark_location = gblevent_config.GBLEVENT_STAGING_PREP
spark_submit_cmd = '/usr/bin/spark-submit ' + spark_location + '  {{execution_date.year}} {{execution_date.month}} {{execution_date.day}}'

start_date_val = gblevent_config.STARTDATE
end_date_val = gblevent_config.ENDDATE

start_date = datetime.strptime(start_date_val, '%Y,%m,%d')
end_date = datetime.strptime(end_date_val, '%Y,%m,%d')

#---------------
default_args = {
    'owner': 'udacity',
    'start_date': start_date,
    'end_date': end_date    
}

dag = DAG('aer_gblevnt_staging_prep',
        default_args=default_args,
        schedule_interval='0 7 * * *')

#dag = DAG(
#        'aer_gblevnt_staging_prep',
#        start_date=datetime.datetime.now())
        
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

get_gblevnt_data = SSHOperator(
    ssh_hook=sshHook,
    task_id="get_gblevnt_data",
    command=spark_submit_cmd,
    dag=dag)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> get_gblevnt_data >> end_operator