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

import boto3
from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators import AERs3ToRedshiftOperator
from airflow.operators import AERDataQualityOperator
from airflow.operators import AERLoadDimOperator
from airflow.operators import AERSqlInserts

#---------------
# Local app imports
#--------------

import gblevent_config as gblevent_config

#--------------
# variable setup
#--------------

#from helpers import AERSqlInserts

#file_loc = "/home/emfdellpc/spark/output/capstone/stagevnt_table"

                     
start_date=datetime.datetime.now()

#--------------

dag = DAG("aer_gblevent_load_stage", start_date=start_date)

def rename_stage_file():

    timenow = datetime.datetime.now()
    timenow_str = datetime.datetime.strftime(timenow, '%Y%m%d')
    
    s3_resource=boto3.resource('s3',
        aws_access_key_id=gblevent_config.KEY,
        aws_secret_access_key= gblevent_config.SECRET)
    
    bucket_name = s3_resource.Bucket('dataeng-capstone-stgevents')
    cnt = 1
    for s3_file in bucket_name.objects.filter(Prefix='stagevnt_table.csv/'): 
        src_key = s3_file.key
        dst_key = "stagevnt_table_bak.csv/" + timenow_str + "_"+ str(cnt)
        cnt = cnt + 1
        s3_resource.Object("dataeng-capstone-stgevents", dst_key).copy_from(CopySource=f'dataeng-capstone-stgevents/{src_key}')
        s3_resource.Object("dataeng-capstone-stgevents", src_key).delete()

# task to load staging table (gbldata_staging) with gbl event data 

load_gblevent_to_redshift = AERs3ToRedshiftOperator(
    task_id="load_gblevent_to_redshift",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials = "aws_credentials",
    table="gbleventstg.gbldata_staging",    
    s3_bucket="dataeng-capstone-stgevents",
    s3_key="s3://dataeng-capstone-stgevents/stagevnt_table.csv"
)

# define data quality checks for staging table

gblevnt_stg_dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM gbleventstg.gbldata_staging WHERE relcd IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM gbleventstg.gbldata_staging WHERE cntrycd IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM gbleventstg.gbldata_staging WHERE actcd IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM gbleventstg.gbldata_staging WHERE evntcd IS NULL", 'expected_result': 0},
    ]
    
# task to run dq checks on (gbldata_staging)  

run_gblevnt_stg_dq_checks = AERDataQualityOperator(
    task_id='Run_gblevnt_stg_dq_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table_names=['gbleventstg.gbldata_staging'],
    dq_checks=gblevnt_stg_dq_checks
)

load_actorclass_stgdim_table = AERLoadDimOperator(
    task_id='Load_actorclass_stgdim_table',
    dag=dag,
    redshift_conn_id="redshift",    
    table="gbleventstg.actorclass_stgdim",
    sql_insert=AERSqlInserts.actorclass_stgdim_table_insert,
    append_mode=False
)

load_evntclass_stgdim_table = AERLoadDimOperator(
    task_id='Load_evntclass_stgdim_table',
    dag=dag,
    redshift_conn_id="redshift",    
    table="gbleventstg.evntclass_stgdim",
    sql_insert=AERSqlInserts.evntclass_stgdim_table_insert,
    append_mode=False
)

load_evntloc_stgdim_table = AERLoadDimOperator(
    task_id='Load_evntloc_stgdim_table',
    dag=dag,
    redshift_conn_id="redshift",    
    table="gbleventstg.evntloc_stgdim",
    sql_insert=AERSqlInserts.evntloc_stgdim_table_insert,
    append_mode=False
)

load_evntcat_stgdim_table = AERLoadDimOperator(
    task_id='Load_evntcat_stgdim_table',
    dag=dag,
    redshift_conn_id="redshift",    
    table="gbleventstg.evntcat_stgdim",
    sql_insert=AERSqlInserts.evntcat_stgdim_table_insert,
    append_mode=False
)

load_gbldata_stgfact_table = AERLoadDimOperator(
    task_id='Load_gbldata_stgfact_table',
    dag=dag,
    redshift_conn_id="redshift",    
    table="gbleventstg.gbldata_stgfact",
    sql_insert=AERSqlInserts.gbldata_stgfact_table_insert,
    append_mode=False
)

# define data quality check task staging fact and dim tables

gblevnt_stg_fact_dim_dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM gbleventstg.evntcat_stgdim WHERE relcd IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM gbleventstg.evntloc_stgdim WHERE cntrycd IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM gbleventstg.actorclass_stgdim WHERE actcd IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM gbleventstg.evntclass_stgdim WHERE evntcd IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM gbleventstg.gbldata_stgfact WHERE globaleventid IS NULL", 'expected_result': 0},
    ]

# task to run dq checks on stg fact and dim tables

run_gblevnt_stg_fact_dim_dq_checks = AERDataQualityOperator(
    task_id='Run_gblevnt_stg_fact_dim_dq_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table_names=['gbleventstg.actorclass_stgdim','gbleventstg.evntclass_stgdim','gbleventstg.evntloc_stgdim','gbleventstg.evntcat_stgdim','gbleventstg.gbldata_stgfact'],
    dq_checks=gblevnt_stg_fact_dim_dq_checks
)

rename_stage_file = PythonOperator(
    task_id = 'rename_stage_file',
    python_callable=rename_stage_file,
    dag=dag
)

load_gblevent_to_redshift >> run_gblevnt_stg_dq_checks
run_gblevnt_stg_dq_checks >> load_actorclass_stgdim_table
run_gblevnt_stg_dq_checks >> load_evntclass_stgdim_table
run_gblevnt_stg_dq_checks >> load_evntloc_stgdim_table
run_gblevnt_stg_dq_checks >> load_evntcat_stgdim_table
run_gblevnt_stg_dq_checks >> load_gbldata_stgfact_table
load_actorclass_stgdim_table >> run_gblevnt_stg_fact_dim_dq_checks
load_evntclass_stgdim_table >> run_gblevnt_stg_fact_dim_dq_checks
load_evntloc_stgdim_table >> run_gblevnt_stg_fact_dim_dq_checks
load_evntcat_stgdim_table >> run_gblevnt_stg_fact_dim_dq_checks
load_gbldata_stgfact_table >> run_gblevnt_stg_fact_dim_dq_checks
run_gblevnt_stg_fact_dim_dq_checks >> rename_stage_file