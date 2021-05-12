#----------------
# Standard library imports
#----------------

import datetime
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

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

import psycopg2

#---------------
# Local app imports
#--------------

from airflow.operators import AERs3ToRedshiftOperator
from airflow.operators import AERLoadDimOperator
from airflow.operators import AERLoadDateDimOperator 
from airflow.operators import AERDataQualityOperator
from airflow.operators import AERSqlInserts

import gblevent_config as gblevent_config

#--------------
# variable setup
#--------------

#from helpers import AERSqlInserts

#file_loc = "/home/emfdellpc/spark/output/capstone/gbldata_fact"
file_loc = gblevent_config.GBLFACT_FILE

sshHook = SSHHook('aws_emr')
#spark_submit_cmd = "/usr/bin/spark-submit --packages org.postgresql:postgresql:42.2.19 /home/hadoop/aer_gblevntdata_fact_prep.py "
spark_location = gblevent_config.GBLEVENT_FACT_PREP
spark_submit_cmd = f"/usr/bin/spark-submit --jars /usr/lib/redshift-jdbc42-2.0.0.4.jar {spark_location} "


start_date=datetime.datetime.now()

#--------------

dag = DAG("aer_gblevent_load_prd", start_date=start_date)

# task to load actorclass dim table

load_actorclass_dim_table = AERLoadDimOperator(
    task_id='Load_actorclass_dim_table',
    dag=dag,
    redshift_conn_id="redshift",    
    table=AERSqlInserts.actorclass_dim_tbl_ins_into_clause,
    sql_insert=AERSqlInserts.actorclass_dim_tbl_ins_val_clause,
    append_mode=True
)

# task to load evntclass dim table

load_evntclass_dim_table = AERLoadDimOperator(
    task_id='Load_evntclass_dim_table',
    dag=dag,
    redshift_conn_id="redshift",    
    table=AERSqlInserts.evntclass_dim_tbl_ins_into_clause,
    sql_insert=AERSqlInserts.evntclass_dim_tbl_ins_val_clause,
    append_mode=True
)

# task to load evntloc dim table

load_evntloc_dim_table = AERLoadDimOperator(
    task_id='Load_evntloc_dim_table',
    dag=dag,
    redshift_conn_id="redshift",    
    table=AERSqlInserts.evntloc_dim_tbl_ins_into_clause,
    sql_insert=AERSqlInserts.evntloc_dim_tbl_ins_val_clause,
    append_mode=True
)

# task to load evntcat dim table

load_evntcat_dim_table = AERLoadDimOperator(
    task_id='Load_evntcat_dim_table',
    dag=dag,
    redshift_conn_id="redshift",    
    table=AERSqlInserts.evntcat_dim_tbl_ins_into_clause,
    sql_insert=AERSqlInserts.evntcat_dim_tbl_ins_val_clause,
    append_mode=True
)

# task to load date dim table

load_date_dim_table = AERLoadDateDimOperator(
    task_id='Load_date_dim_table',
    dag=dag,
    redshift_conn_id="redshift",    
    call_stored_proc="call gblevent.load_dateDim(CAST('2019-01-01' as date), CAST('2019-06-03' as date));"
)

# define data quality check task dim tables

gblevnt_dim_dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM gblevent.evntcat_dim WHERE relcd IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM gblevent.evntloc_dim WHERE cntrycd IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM gblevent.actorclass_dim WHERE actcd IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM gblevent.evntclass_dim WHERE evntcd IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM gblevent.date_dim WHERE datekey IS NULL", 'expected_result': 0},
    ]

# task to run dq checks on dim tables

run_gblevnt_dim_dq_checks = AERDataQualityOperator(
    task_id='Run_gblevnt_dim_dq_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table_names=['gblevent.actorclass_dim','gblevent.evntclass_dim','gblevent.evntloc_dim','gblevent.evntcat_dim','gblevent.date_dim'],
    dq_checks=gblevnt_dim_dq_checks
)

# task to create gbldata fact file
get_gblevnt_fact = SSHOperator(
    ssh_hook=sshHook,
    task_id="get_gblevnt_fact",
    command = spark_submit_cmd,
    dag=dag
)

load_gblfct_to_redshift = AERs3ToRedshiftOperator(
    task_id="load_gblfct_to_redshift",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials = "aws_credentials",
    table="gblevent.gbldata_fact",    
    s3_bucket="dataeng-capstone",
    s3_key="s3://dataeng-capstone-gbldata/gbldata_fact.csv"
)

load_actorclass_dim_table >> run_gblevnt_dim_dq_checks
load_evntclass_dim_table >> run_gblevnt_dim_dq_checks
load_evntloc_dim_table >> run_gblevnt_dim_dq_checks
load_evntcat_dim_table >> run_gblevnt_dim_dq_checks
load_date_dim_table >> run_gblevnt_dim_dq_checks
run_gblevnt_dim_dq_checks >> get_gblevnt_fact
get_gblevnt_fact >> load_gblfct_to_redshift
#load_gbldata_fact_table >> run_gblevnt_dim_dq_checks