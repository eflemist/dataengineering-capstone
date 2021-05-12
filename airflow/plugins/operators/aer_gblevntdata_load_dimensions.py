from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class AERLoadDimOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql = """
        INSERT INTO {}
        {}
     """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",    
                 table="",
                 sql_insert="",
                 append_mode="",
                 *args, **kwargs):

        super(AERLoadDimOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql_insert=sql_insert
        self.append_mode=append_mode


    def execute(self, context):
        self.log.info('AERLoadDimOperator started...')
        
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_mode==False:
            self.log.info('AERLoadDimOperator: running delete function')
            delete_sql = f'DELETE FROM {self.table}'
            redshift_hook.run(delete_sql)
 
        self.log.info('AERLoadDimOperator: inserting data')
        redshift_hook.run(AERLoadDimOperator.insert_sql.format(self.table,self.sql_insert))
