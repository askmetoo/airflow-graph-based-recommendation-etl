from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.data_helper import DataHelper

class ParseActivityJsonOperator(BaseOperator):

    ui_color = '#A9CCE3'

    @apply_defaults
    def __init__(self,
                 aws_conn_id,
                 s3_region,
                 s3_bucket,
                 s3_key_in,
                 s3_key_out,
                 activity,
                 *args, **kwargs):

        super(ParseActivityJsonOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.s3_region = s3_region
        self.s3_bucket = s3_bucket
        self.s3_key_in = s3_key_in
        self.s3_key_out = s3_key_out
        self.activity = activity

    def execute(self, context):
        aws = AwsHook(aws_conn_id=self.aws_conn_id)
        s3_client = aws.get_client_type('s3', region_name=self.s3_region)
        s3_resource = aws.get_resource_type('s3', region_name=self.s3_region)

        self.log.info(f"Parsing {self.activity} events from s3://{self.s3_bucket}/{self.s3_key_in}.")
        with DataHelper.buffer_s3_object_as_file(s3_client, self.s3_bucket, self.s3_key_in) as f:
            data = DataHelper.parse_activity_json_to_df(json_file, self.activity)
            DataHelper.write_df_to_tsv_in_s3(s3_resource, data, self.s3_bucket, self.s3_key_out)
        self.log.info(f"Wrote {self.activity} eveents to s3://{self.s3_bucket}/{self.s3_key_out}.")