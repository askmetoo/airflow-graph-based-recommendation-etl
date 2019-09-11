from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.data_helper import DataHelper

class DropColumnOperator(BaseOperator):

    ui_color = '#F9E79F'

    @apply_defaults
    def __init__(self,
                 aws_conn_id,
                 s3_region,
                 s3_bucket,
                 s3_key_in,
                 s3_key_out,
                 drop_column,
                 *args, **kwargs):

        super(DropColumnOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.s3_region = s3_region
        self.s3_bucket = s3_bucket
        self.s3_key_in = s3_key_in
        self.s3_key_out = s3_key_out
        self.drop_column = drop_column

    def execute(self, context):
        aws = AwsHook(aws_conn_id=self.aws_conn_id)
        s3_client = aws.get_client_type('s3', region_name=self.s3_region)
        s3_resource = aws.get_resource_type('s3', region_name=self.s3_region)

        data = DataHelper.read_tsv_from_s3_to_df(s3_client, self.s3_bucket, self.s3_key_in)
        self.log.info(f"Read tsv file s3://{self.s3_bucket}/{self.s3_key_in} into dataframe.")
        data_with_dropped = DataHelper.drop_column(data, self.drop_column)
        DataHelper.write_df_to_tsv_in_s3(s3_resource, data_with_dropped, self.s3_bucket, self.s3_key_out)
        self.log.info(f"Wrote tsv file with {self.drop_column} dropped to s3://{self.s3_bucket}/{self.s3_key_out}.")