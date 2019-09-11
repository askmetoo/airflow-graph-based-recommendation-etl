from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.data_helper import DataHelper

class GetIndicatorColumnsOperator(BaseOperator):

    ui_color = '#82E0AA'

    @apply_defaults
    def __init__(self,
                 aws_conn_id,
                 s3_region,
                 s3_bucket,
                 s3_key_in,
                 s3_key_out,
                 indicator_column,
                 sep=None,
                 *args, **kwargs):

        super(GetIndicatorColumnsOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.s3_region = s3_region
        self.s3_bucket = s3_bucket
        self.s3_key_in = s3_key_in
        self.s3_key_out = s3_key_out
        self.indicator_column = indicator_column
        self.sep = sep

    def execute(self, context):
        aws = AwsHook(aws_conn_id=self.aws_conn_id)
        s3_client = aws.get_client_type('s3', region_name=self.s3_region)
        s3_resource = aws.get_resource_type('s3', region_name=self.s3_region)

        data = DataHelper.read_csv_from_s3_to_df(s3_client, self.s3_bucket, self.s3_key_in)
        self.log.info(f"Read csv file s3://{self.s3_bucket}/{self.s3_key_in} into dataframe.")
        data_with_dummies = DataHelper.get_dummy_colums(data, self.indicator_column, sep=self.sep)
        self.log.info(f"Created dummy fields for column {self.indicator_column}.")
        DataHelper.write_df_to_csv_in_s3(s3_resource, data_with_dummies, self.s3_bucket, self.s3_key_in)
        self.log.info(f"Wrote updated data back to s3://{self.s3_bucket}/{self.s3_key_out}.")
        