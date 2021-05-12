from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class AERDataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_names="",
                 dq_checks="",
                 *args, **kwargs):

        super(AERDataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id 
        self.table_names = table_names
        self.dq_checks = dq_checks

    def execute(self, context):
        self.log.info('AERDataQualityOperator started...')
        
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        fail_null_chk = []
        error_cnt = 0
        
        for check in self.dq_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
 
            records_query = redshift_hook.get_records(sql)[0]
 
            if exp_result != records_query[0]:
                error_cnt += 1
                fail_null_chk.append(sql)
 
        if error_cnt > 0:
            self.log.info('Tests failed')
            self.log.info(fail_null_chk)
            raise ValueError('Data quality check failed')
        
        for table in self.table_names:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
        
            if len(records) < 1 or len(records[0]) < 1:
                self.log.info(f"Failed data quality record count on table {table} no results")       
                raise ValueError(f"Data quality check failed. {table} returned no results")
            
            num_records = records[0][0]
        
            if num_records < 1:
                self.log.info(f"Failed data quality record count on table {table} 0 records")       
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
        
            self.log.info(f"Passed data quality check. Table {table} has {records[0][0]} records")