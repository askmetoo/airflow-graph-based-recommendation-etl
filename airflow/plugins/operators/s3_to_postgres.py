from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.data_helper import DataHelper

class LoadS3ToPostgresOperator(BaseOperator):

    ui_color = '#D2B4DE'

    @apply_defaults
    def __init__(self,
                 aws_conn_id,
                 postgres_conn_id,
                 table,
                 s3_region,
                 s3_bucket,
                 s3_key,
                 *args, **kwargs):

        super(LoadS3ToPostgresOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.s3_region = s3_region
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        aws = AwsHook(aws_conn_id=self.aws_conn_id)
        s3_client = aws.get_client_type('s3', region_name=self.s3_region)
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        self.log.info(f"Loading file s3://{self.s3_bucket}/{self.s3_key} into table {self.table}.")
        with DataHelper.buffer_s3_object_as_file(s3_client, self.s3_bucket, self.s3_key) as f:
            postgres.bulk_load(self.table, f)
        self.log.info(f"s3://{self.s3_bucket}/{self.s3_key} loaded into table {self.table} sucesfully.")