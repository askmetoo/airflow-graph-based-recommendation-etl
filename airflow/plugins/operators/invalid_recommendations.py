from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.data_helper import DataHelper
from helpers.recommendation_helper import RecommendationHelper

class DropInvalidConnectionsOperator(BaseOperator):

    ui_color = '#A3E4D7'

    @apply_defaults
    def __init__(self,
                 aws_conn_id,
                 s3_region,
                 s3_bucket,
                 conn_type,
                 s3_key_sec_deg,
                 s3_key_existing_conn,
                 s3_key_out,
                 *args, **kwargs):

        super(DropInvalidConnectionsOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.s3_region = s3_region
        self.s3_bucket = s3_bucket
        self.conn_type = conn_type
        self.s3_key_sec_deg = s3_key_sec_deg
        self.s3_key_existing_conn = s3_key_existing_conn
        self.s3_key_out = s3_key_out

    def execute(self, context):
        aws = AwsHook(aws_conn_id=self.aws_conn_id)
        s3_client = aws.get_client_type('s3', region_name=self.s3_region)
        s3_resource = aws.get_resource_type('s3', region_name=self.s3_region)

        data_sec_deg = DataHelper.read_tsv_from_s3_to_df(s3_client, self.s3_bucket, self.s3_key_in_sec_deg)
        self.log.info(f"Read tsv file s3://{self.s3_bucket}/{self.s3_key_in_sec_deg} into dataframe.")
        data_existing_conn = DataHelper.read_tsv_from_s3_to_df(s3_client, self.s3_bucket, self.s3_key_existing_conn)
        self.log.info(f"Read tsv file s3://{self.s3_bucket}/{self.s3_key_existing_conn} into dataframe.")

        sec_deg_conn_valid = RecommendationHelper.remove_invalid_recommendations(data_sec_deg, data_existing_conn, conn_type)
        DataHelper.write_df_to_tsv_in_s3(s3_resource, sec_deg_conn_valid, self.s3_bucket, self.s3_key_out)
        self.log.info(f"Wrote valid second degree connections tsv file to s3://{self.s3_bucket}/{self.s3_key_out}.")