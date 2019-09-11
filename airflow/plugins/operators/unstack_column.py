from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.data_helper import DataHelper

class UnstackColumnOperator(BaseOperator):

    ui_color = '#E6B0AA'

    @apply_defaults
    def __init__(self,
                 aws_conn_id,
                 s3_region,
                 s3_bucket,
                 s3_key_in,
                 s3_key_out,
                 id_column,
                 unstack_column,
                 *args, **kwargs):

        super(UnstackColumnOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.s3_region = s3_region
        self.s3_bucket = s3_bucket
        self.s3_key_in = s3_key_in
        self.s3_key_out = s3_key_out
        self.id_column = id_column
        self.unstack_column = unstack_column

    def execute(self, context):
        aws = AwsHook(aws_conn_id=self.aws_conn_id)
        s3_client = aws.get_client_type('s3', region_name=self.s3_region)
        s3_resource = aws.get_resource_type('s3', region_name=self.s3_region)

        data = DataHelper.read_tsv_from_s3_to_df(s3_client, self.s3_bucket, self.s3_key_in)
        self.log.info(f"Read tsv file s3://{self.s3_bucket}/{self.s3_key_in} into dataframe.")
        unstacked_data = DataHelper.unstack_df_column(data, self.id_column, self. unstack_column)
        DataHelper.write_df_to_tsv_in_s3(s3_resource, unstacked_data, self.s3_bucket, self.s3_key_out)
        self.log.info(f"Wrote tsv file with unstacked {self.unstack_column} to s3://{self.s3_bucket}/{self.s3_key_out}.")