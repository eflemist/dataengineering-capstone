from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class AERLoadDateDimOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",    
                 call_stored_proc="",
                 *args, **kwargs):

        super(AERLoadDateDimOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.call_stored_proc=call_stored_proc
  


    def execute(self, context):
        self.log.info('AERLoadDateDimOperator started, inserting data...')
        
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift_hook.run(self.call_stored_proc)