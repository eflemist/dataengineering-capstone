#from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class AERs3ToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}' credentials 'aws_access_key_id={};aws_secret_access_key={}'
        FORMAT AS CSV
        DELIMITER AS '\t'
        region 'us-east-1'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 *args, **kwargs):

        super(AERs3ToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
 
    def execute(self, context):
        self.log.info('AERs3ToRedshiftOperator started...')
        
        aws_hook = S3Hook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Copying data from S3 bucket to Redshift")
        rendered_key = self.s3_key.format(**context)
        self.log.info(rendered_key)
        #s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        s3_path = rendered_key
        self.log.info(f"table: {self.table} path: {s3_path} access: {credentials.access_key} secret: {credentials.secret_key}")        
        formatted_sql = AERs3ToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key
        )
        redshift_hook.run(formatted_sql)




