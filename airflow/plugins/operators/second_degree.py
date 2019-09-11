from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.data_helper import DataHelper
from helpers.recommendation_helper import RecommendationHelper

class GetSecDegConnectionsOperator(BaseOperator):

    ui_color = '##D7BDE2'

    @apply_defaults
    def __init__(self,
                 aws_conn_id,
                 s3_region,
                 s3_bucket,
                 s3_key_in_first_deg,
                 s3_key_in_sec_deg,
                 s3_key_out,
                 *args, **kwargs):

        super(GetSecDegConnectionsOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.s3_region = s3_region
        self.s3_bucket = s3_bucket
        self.s3_key_in_first_deg = s3_key_in_first_deg
        self.s3_key_in_sec_deg = s3_key_in_sec_deg
        self.s3_key_out = s3_key_out

    def execute(self, context):
        aws = AwsHook(aws_conn_id=self.aws_conn_id)
        s3_client = aws.get_client_type('s3', region_name=self.s3_region)
        s3_resource = aws.get_resource_type('s3', region_name=self.s3_region)

        data_first_deg = DataHelper.read_tsv_from_s3_to_df(s3_client, self.s3_bucket, self.s3_key_in_first_deg)
        self.log.info(f"Read tsv file s3://{self.s3_bucket}/{self.s3_key_in_first_deg} into dataframe.")
        data_sec_deg = DataHelper.read_tsv_from_s3_to_df(s3_client, self.s3_bucket, self.s3_key_in_sec_deg)
        self.log.info(f"Read tsv file s3://{self.s3_bucket}/{self.s3_key_in_sec_deg} into dataframe.")

        sec_deg_connections = RecommendationHelper.get_second_degree_connections(data_first_deg, data_sec_deg)
        DataHelper.write_df_to_tsv_in_s3(s3_resource, sec_deg_connections, self.s3_bucket, self.s3_key_out)
        self.log.info(f"Wrote second degree connections tsv file to s3://{self.s3_bucket}/{self.s3_key_out}.")