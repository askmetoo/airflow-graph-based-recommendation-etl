import pandas as pd
import io
import json

class DataHelper:

    def read_csv_from_s3_to_df(s3_client, bucket, key):
        """
        Function to read a csv file from s3 into a pandas df
        """
        csv_obj = s3_client.get_object(Bucket=bucket, Key=key)
        body = csv_obj['Body']
        csv_string = body.read().decode('utf-8')
        df = pd.read_csv(io.StringIO(csv_string))
        return df

    def write_df_to_csv_in_s3(s3_resource, df, bucket, key):
        """
        Function to write a pandas df to a csv file on s3
        """
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())

    def read_tsv_from_s3_to_df(s3_client, bucket, key):
        """
        Function to read a tsv file from s3 into a pandas df
        """
        csv_obj = s3_client.get_object(Bucket=bucket, Key=key)
        body = csv_obj['Body']
        csv_string = body.read().decode('utf-8')
        df = pd.read_csv(io.StringIO(csv_string), sep='\t')
        return df
        
    def write_df_to_tsv_in_s3(s3_resource, df, bucket, key):
        """
        Function to write a pandas df to a tsv file on s3
        """
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, sep='\t')
        s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())

    def get_dummy_colums(df, column, sep):
        """
        Function to split categorical data field into dummy columns
        """
        df_with_dummies = df.copy()
        df_with_dummies.loc[:, column] = df_with_dummies.loc[: , column].str.replace(' ', '_').str.replace('-', '_').str.lower()
        dummies = df_with_dummies.loc[:, column].str.get_dummies(sep=sep)
        df_with_dummies.join(dummies, inplace=True)
        df_with_dummies.drop(column, axis=1, inplace=True)
        return df_with_dummies

    def buffer_s3_object_as_file(s3_client, bucket, key):
        """
        Function to buffer an object from s3 into a temporary file
        """
        csv_obj = s3_client.get_object(Bucket=bucket, Key=key)
        body = csv_obj['Body']
        csv_string = body.read().decode('utf-8')
        return io.StringIO(csv_string)

    def parse_activity_json_to_df(json_file, activity):
        """
        Function to parse our specific activity events from a json log of different events
        """
        activity_data = []
        for line in json_file.readlines(): 
            event = json.loads(line)
            if event['action'] == activity:
                activity_data.append(event)
        cols = sorted(activity_data[0].keys(), reverse=True)
        activity_df = pd.DataFrame(activity_data, cols)
        activity_df.drop('action', axis=1, inplace=True)
        return activity_df

    def unstack_df_column(df, id_col, unstack_col):
        """
        Function to unroll a column of comma separated values associated with a specific id column
        """
        unstacked = []
        for user, unstack_ids in df.loc[:, (id_col, unstack_col)].values:
            if not unstack_ids:
                continue
            msg_unstack = [int(unstack_id) for unstack_id in unstack_ids.split(",")]
            for unstack in msg_unstack:
                unstacked.append([user, unstack])

        unstacked = pd.DataFrame(unstacked, columns=[id_col, id_col[:-1]])
        return unstacked

    def drop_df_column(df, column):
        """
        Funciton to drop a column from a df
        """
        return df.drop(column, axis=1)

    def get_joined_data_from_dfs(df_left, df_right, left_on, right_on, suffix, output_columns):
        """
        Fucntion to join two dataframes and output some joined result into another dataframe
        """
        joined = df_left.merge(df_right, how='left', left_on=left_on, right_on=right_on, suffixes=("", suffix))
        joined = joined.loc[:, output_columns]
        return joined

    def generate_all_keys_from_s3_with_prefix(s3_client, bucket, prefix):
        """
        Function to generate all keys from s3 with specified prefix 
        """
        while True:
            resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            for obj in resp['Contents']:
                key = obj['Key']
                yield key
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break

    def combine_engagement_dfs(dfs, column_names, weight_fn):
        """
        function that takes a list of dfs with a users id and an engaged with id and combines them 
        and adds a weight value.
        """
        for df in dfs:
            df.columns = column_names

        combined_df = pd.concat(dfs, axis=0)
        combined_df.loc[:, 'weight'] = combined_df.apply(weight_fn, axis=1)
        return combined_df
