from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.data_helper import DataHelper

class AggregateEngagementsOperator(BaseOperator):

    ui_color = '#EDBB99'

    @apply_defaults
    def __init__(self,
                 aws_conn_id,
                 s3_region,
                 s3_bucket,
                 engament_type,
                 s3_key_out,
                 *args, **kwargs):

        super(AggregateEngagementsOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.s3_region = s3_region
        self.s3_bucket = s3_bucket
        self.engament_type = engament_type
        self.s3_key_out = s3_key_out

    def execute(self, context):
        aws = AwsHook(aws_conn_id=self.aws_conn_id)
        s3_client = aws.get_client_type('s3', region_name=self.s3_region)
        s3_resource = aws.get_resource_type('s3', region_name=self.s3_region)

        engagement_dfs = []
        for key in DataHelper.generate_all_keys_from_s3_with_prefix(s3_client, self.s3_bucket, f"{self.engagement_type}/"):
            df = DataHelper.read_tsv_from_s3_to_df(s3_client, self.s3_bucket, key)
            self.log.info(f"Read tsv file s3://{self.s3_bucket}/{key} into dataframe.")
            engagement_dfs.append(df)
        
        all_engagement_df = DataHelper.combine_engagement_dfs(engagement_dfs, ['user_id', 'engaged_with_id'], lambda x: 1)
        DataHelper.write_df_to_tsv_in_s3(s3_resource, all_engagement_df, self.s3_bucket, self.s3_key_out)
        self.log.info(f"Wrote combined engagement tsv file to s3://{self.s3_bucket}/{self.engagement_type}/{self.s3_key_out}.")