from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.data_helper import DataHelper

class JoinActivityDataOperator(BaseOperator):

    ui_color = '#AED6F1'

    @apply_defaults
    def __init__(self,
                 aws_conn_id,
                 s3_region,
                 s3_bucket,
                 s3_key_in_left,
                 s3_key_in_right,
                 s3_key_out,
                 left_on_column,
                 right_on_column,
                 suffix_name,
                 output_columns,
                 *args, **kwargs):

        super(JoinActivityDataOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.s3_region = s3_region
        self.s3_bucket = s3_bucket
        self.s3_key_in_left = s3_key_in_left
        self.s3_key_in_right = s3_key_in_right
        self.s3_key_out = s3_key_out
        self.left_on_column = left_on_column
        self.right_on_column = right_on_column
        self.suffix_name = suffix_name
        self.output_columns = output_columns

    def execute(self, context):
        aws = AwsHook(aws_conn_id=self.aws_conn_id)
        s3_client = aws.get_client_type('s3', region_name=self.s3_region)
        s3_resource = aws.get_resource_type('s3', region_name=self.s3_region)

        data_left = DataHelper.read_tsv_from_s3_to_df(s3_client, self.s3_bucket, self.s3_key_in_left)
        self.log.info(f"Read tsv file s3://{self.s3_bucket}/{self.s3_key_in} into dataframe.")
        data_right = DataHelper.read_tsv_from_s3_to_df(s3_client, self.s3_bucket, self.s3_key_in_right)
        self.log.info(f"Read tsv file s3://{self.s3_bucket}/{self.s3_key_in} into dataframe.")

        joined_data = DataHelper.get_joined_data_from_dfs(
            data_left, 
            data_right, 
            self.left_on_column, 
            self.right_on_column, 
            self.suffix_name, 
            self.output_columns
        )
        DataHelper.write_df_to_tsv_in_s3(s3_resource, joined_data, self.s3_bucket, self.s3_key_out)
        self.log.info(f"Wrote tsv file with joined columns {self.output_columns} dropped to s3://{self.s3_bucket}/{self.s3_key_out}.")