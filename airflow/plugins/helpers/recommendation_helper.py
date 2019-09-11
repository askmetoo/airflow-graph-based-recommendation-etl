import itertools as it
import numpy as np
import pandas as pd

class RecommendationHelper:
    
    def get_top_n_closest(df, n):
        """
        function that takes a df of engagements and returns the top n most engaged with 
        for each user
        """
        df_eng_count = df.groupby(["user_id", "engaged_with_id"]).agg({'weight': sum}).reset_index()
        df_eng_count.sort_values(['weight', 'engaged_with_id'], ascending=[False, True], inplace=True)
        n_closest = df_eng_count.groupby('user_id').head(n).sort_values('user_id')
        return n_closest

    def get_second_degree_connections(df_first, df_sec):
        """
        function that joins second degree connections to a user
        """
        sec_deg_cnx = df_first.merge(df_sec, how='left', left_on='engaged_with_id', right_on='user_id', suffixes=('', '_sec_deg'))
        sec_deg_cnx.drop(["user_id_sec_deg", "weight"], inplace=True, axis=1)
        return sec_deg_cnx

    def remove_invalid_recommendations(df_rec, df_invalid, conn_type):
        """
        function that removes recommendations based on if the user is already connected to that recommendation
        or if the recommendation is the user themselves
        """
        df_temp = pd.merge(
            df_rec, 
            df_invalid, 
            left_on=["user_id", "engaged_with_id_sec_deg"],
            right_on=["user_id", "engaged_with_id"],
            how="left", 
            indicator=True, 
            suffixes=("", "_connected")
        )
        df_rec_valid = df_temp[df_temp["_merge"] == "left_only"].copy()
        df_rec_valid.drop(["_merge", "engaged_with_id_connected"], inplace=True, axis=1)
        if conn_type == 'users':
            identity_mask = df_rec_valid["user_id"] != df_rec_valid["engaged_with_id_sec_deg"]
            df_rec_valid = df_rec_valid[identity_mask].copy()
        return df_rec_valid

    def get_top_n_recommendations(df, n):
        sec_deg_eng_count = df.groupby(["user_id", "engaged_with_id_sec_deg"]).agg({"weight_sec_deg": sum}).reset_index()
        sec_deg_eng_count.sort_values(["weight_sec_deg", "user_id"], ascending=[False, True], inplace=True)
        df_recs = sec_deg_eng_count.groupby('user_id').head(n).sort_values('user_id')
        df_recs.columns = ['user_id', 'rec_id', 'weight']
        return df_recs

    def get_top_n_recommedation_reasons(df, n):
        sec_deg_eng_count_w_reason = df.groupby([
            "user_id", 
            "engaged_with_id_sec_deg"
            ]).agg({"weight_sec_deg": sum, "engaged_with_id": lambda x: tuple(x)}).reset_index()
        sec_deg_eng_count_w_reason.sort_values(["weight_sec_deg", "user_id"], ascending=[False, True], inplace=True)
        sec_deg_eng_count_w_reason.drop('weight_sec_deg', axis=1, inplace=True)
        agg_rec_reasons = sec_deg_eng_count_w_reason.groupby('user_id').head(10)
        df_rec_reasons = pd.DataFrame({
            "user_id": np.repeat(agg_rec_reasons['user_id'].values, agg_rec_reasons['engaged_with_id'].str.len()),
            "rec_id": np.repeat(agg_rec_reasons['engaged_with_id_sec_deg'].values, agg_rec_reasons['engaged_with_id'].str.len()),
            "reason_id": list(it.chain.from_iterable(agg_rec_reasons['engaged_with_id']))
        })
        return df_rec_reasons
