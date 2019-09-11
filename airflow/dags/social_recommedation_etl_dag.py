from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (GetIndicatorColumnsOperator,CsvToTsvOperator,
                               LoadS3ToPostgresOperator, ParseActivityJsonOperator,
                               UnstackColumnOperator, DropColumnOperator, 
                               JoinActivityDataOperator, AggregateEngagementsOperator,
                               GetStrongestConnectionsOperator, GetSecDegConnectionsOperator,
                               DropInvalidConnectionsOperator, GetGraphRecommendationsOperator,
                               GetGraphRecReasonsOperator, DataQualityOperator) 


default_args = {
    'owner': 'garrett',
    'start_date': datetime(2019, 9, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('social_recommendation_etl',
          default_args=default_args,
          description="""
          Load user, room and engagement data into Postgres, transform into
          reccomendations based on engagements (with reasons for rec) and load
          recommendations and reason into Postgres to be served into a recommendation
          module on stocktwts.
          """,
          schedule_interval='0 6 * * *'
        )

start_operator = DummyOperator(task_id='begin_execution',  dag=dag)

parse_message_activities = ParseActivityJsonOperator(
    task_id='parse_message_activities',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key_in='activity.json',
    s3_key_out='messages.tsv',
    activity='message',
    dag=dag
)

parse_like_activities = ParseActivityJsonOperator(
    task_id='parse_like_activities',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key_in='activity.json',
    s3_key_out='likes.tsv',
    activity='like',
    dag=dag
)

parse_follow_activities = ParseActivityJsonOperator(
    task_id='parse_follow_activities',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key_in='activity.json',
    s3_key_out='users/follows.tsv',
    activity='follow',
    dag=dag
)

parse_subscribe_activities = ParseActivityJsonOperator(
    task_id='parse_subscribe_activities',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key_in='activity.json',
    s3_key_out='rooms/subscribes.tsv',
    activity='subscribe',
    dag=dag
)

unstack_message_mentions = UnstackColumnOperator(
    task_id='unstack_message_mentions',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key_in='messages.tsv',
    s3_key_out='users/mentions.tsv',
    id_column='user_id',
    unstack_column='mention_ids',
    dag=dag
)

drop_message_mentions = DropColumnOperator(
    task_id='drop_mentions_from_room_messages',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key_in='messages.tsv',
    s3_key_out='room/messages.tsv',
    drop_column='mention_ids',
    dag=dag
)

join_likes_to_users = JoinActivityDataOperator(
    task_id='join_likes_with_users',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key_in_left='likes.tsv',
    s3_key_in_right='messages.tsv',
    s3_key_out='users/liked_users.tsv',
    left_on_column='message_id',
    right_on_column='message_id',
    suffix_name='_liked',
    output_columns=('user_id', 'user_id_liked'),
    dag=dag
)

join_likes_to_rooms = JoinActivityDataOperator(
    task_id='join_likes_with_rooms',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key_in_left='likes.tsv',
    s3_key_in_right='messages.tsv',
    s3_key_out='rooms/liked_rooms.tsv',
    left_on_column='message_id',
    right_on_column='message_id',
    suffix_name='_liked',
    output_columns=('user_id', 'room_id_liked'),
    dag=dag
)

aggregate_user_engagements = AggregateEngagementsOperator(
    task_id='aggregate_user_engagements',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    engament_type='users',
    s3_key_out='user_engagements.tsv',
    dag=dag
)

aggregate_room_engagements = AggregateEngagementsOperator(
    task_id='aggregate_room_engagements',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    engament_type='rooms',
    s3_key_out='room_engagements.tsv',
    dag=dag
)

get_closest_users = GetStrongestConnectionsOperator(
    task_id='get_closest_users',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key_in='users/user_engagements.tsv',
    n_strongest=10,
    s3_key_out='users/closest_users.tsv',
    dag=dag
)

get_closest_rooms = GetStrongestConnectionsOperator(
    task_id='get_closest_rooms',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key_in='rooms/room_engagements.tsv',
    n_strongest=10,
    s3_key_out='rooms/closest_rooms.tsv',
    dag=dag
)

get_second_degree_users = GetSecDegConnectionsOperator(
    task_id='get_second_degree_users',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key_in_first_deg='users/closest_users.tsv',
    s3_key_in_sec_deg='users/closest_users.tsv',
    s3_key_out='users/sec_deg_users.tsv',
    dag=dag
)

get_second_degree_rooms = GetSecDegConnectionsOperator(
    task_id='get_second_degree_rooms',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key_in_first_deg='rooms/closest_users.tsv',
    s3_key_in_sec_deg='rooms/closest_rooms.tsv',
    s3_key_out='rooms/sec_deg_rooms.tsv',
    dag=dag
)

drop_invalid_sec_deg_users = DropInvalidConnectionsOperator(
    task_id='drop_invalid_sec_deg_users',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    conn_type='users',
    s3_key_sec_deg='users/sec_deg_users.tsv',
    s3_key_existing_conn='users/follows.tsv',
    s3_key_out='users/sec_deg_users_valid.tsv',
    dag=dag
)

drop_invalid_sec_deg_rooms = DropInvalidConnectionsOperator(
    task_id='drop_invalid_sec_deg_rooms',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    conn_type='rooms',
    s3_key_sec_deg='rooms/sec_deg_rooms.tsv',
    s3_key_existing_conn='rooms/subscribes.tsv',
    s3_key_out='rooms/sec_deg_rooms_valid.tsv',
    dag=dag
)

get_user_recommendations = GetGraphRecommendationsOperator(
    task_id='get_user_recommendations',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key_sec_deg='users/sec_deg_users_valid.tsv',
    n_recs=10,
    s3_key_out='users/user_recs.tsv',
    dag=dag
)

get_room_recommendations = GetGraphRecommendationsOperator(
    task_id='get_room_recommendations',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key_sec_deg='rooms/sec_deg_rooms_valid.tsv',
    n_recs=10,
    s3_key_out='rooms/room_recs.tsv',
    dag=dag
)

get_user_rec_reasons= GetGraphRecReasonsOperator(
    task_id='get_user_rec_reasons',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key_sec_deg='users/sec_deg_users_valid.tsv',
    n_recs=10,
    s3_key_out='users/user_rec_reasons.tsv',
    dag=dag
)

get_room_rec_reasons= GetGraphRecReasonsOperator(
    task_id='get_room_rec_reasons',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key_sec_deg='rooms/sec_deg_rooms_valid.tsv',
    n_recs=10,
    s3_key_out='rooms/room_rec_reasons.tsv',
    dag=dag
)

load_user_recs_to_postgres = LoadS3ToPostgresOperator(
    task_id='load_user_recs',
    aws_conn_id='dev_aws',
    postgres_conn_id='dev_postgres',
    table='user_recs',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key='users/user_recs.tsv',
    dag=dag
)

load_room_recs_to_postgres = LoadS3ToPostgresOperator(
    task_id='load_room_recs',
    aws_conn_id='dev_aws',
    postgres_conn_id='dev_postgres',
    table='room_recs',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key='rooms/room_recs.tsv',
    dag=dag
)

load_user_rec_reasons_to_postgres = LoadS3ToPostgresOperator(
    task_id='load_user_rec_reasons',
    aws_conn_id='dev_aws',
    postgres_conn_id='dev_postgres',
    table='user_rec_reasons',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key='users/user_rec_reasons.tsv',
    dag=dag
)

load_room_rec_reasons_to_postgres = LoadS3ToPostgresOperator(
    task_id='load_room_rec_reasons',
    aws_conn_id='dev_aws',
    postgres_conn_id='dev_postgres',
    table='room_rec_reasons',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key='rooms/room_rec_reasons.tsv',
    dag=dag
)

get_user_indicator_columns = GetIndicatorColumnsOperator(
    task_id='get_user_indicator_columns',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key_in='stocktwits_users.csv',
    s3_key_out='stocktwits_users_w_ind.csv',
    indicator_column='assets_traded',
    sep=',',
    dag=dag
)

get_room_indicator_columns = GetIndicatorColumnsOperator(
    task_id='get_room_indicator_columns',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key_in='stocktwits_rooms.csv',
    s3_key_out='stocktwits_rooms_w_ind.csv',
    indicator_column='topics',
    sep=',',
    dag=dag
)

convert_users_to_tsv = CsvToTsvOperator(
    task_id='convert_users_to_tsv',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key_in='stocktwits_users_w_ind.csv',
    s3_key_out='stocktwits_users_w_ind.tsv',
    dag=dag
)

convert_rooms_to_tsv = CsvToTsvOperator(
    task_id='convert_rooms_to_tsv',
    aws_conn_id='dev_aws',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key_in='stocktwits_rooms_w_ind.csv',
    s3_key_out='stocktwits_rooms_w_ind.tsv',
    dag=dag
)

load_users_to_postgres = LoadS3ToPostgresOperator(
    task_id='load_user_metadata',
    aws_conn_id='dev_aws',
    postgres_conn_id='dev_postgres',
    table='users',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key='stocktwits_users_w_ind.tsv',
    dag=dag
)

load_rooms_to_postgres = LoadS3ToPostgresOperator(
    task_id='load_room_metadata',
    aws_conn_id='dev_aws',
    postgres_conn_id='dev_postgres',
    table='rooms',
    s3_region='us-east-1',
    s3_bucket='glh-dend-capstone',
    s3_key='stocktwits_rooms_w_ind.tsv',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    postgres_conn_id='dev_postgres',
    test_cases=[
        ("SELECT COUNT(user_id) from user_recs WHERE user_id IS NULL OR rec_id IS NULL", 0),
        ("SELECT COUNT(user_id) from user_rec_reasons WHERE user_id IS NULL OR rec_id IS NULL", 0),
        ("SELECT COUNT(user_id) from room_recs WHERE user_id IS NULL OR rec_id IS NULL", 0),
        ("SELECT COUNT(user_id) from room_rec_reasons WHERE user_id IS NULL OR rec_id IS NULL", 0),
        ("SELECT COUNT(id) FROM users", 5000),
        ("SELECT COUNT(id) FROM rooms", 1000)
    ],
    dag=dag
)

end_operator = DummyOperator(task_id='stop_execution',  dag=dag)

# etl flow
start_operator >> parse_message_activities 
start_operator >> parse_like_activities 
start_operator >> parse_follow_activities 
start_operator >> parse_subscribe_activities 
start_operator >> get_user_indicator_columns 
start_operator >> get_room_indicator_columns

parse_message_activities >> unstack_message_mentions
parse_message_activities >> drop_message_mentions
parse_message_activities >> join_likes_to_users
parse_like_activities >> join_likes_to_users
parse_message_activities >> join_likes_to_rooms
parse_like_activities >> join_likes_to_rooms
parse_follow_activities >> aggregate_user_engagements 
unstack_message_mentions >> aggregate_user_engagements
join_likes_to_users >> aggregate_user_engagements
parse_subscribe_activities >> aggregate_room_engagements
drop_message_mentions >> aggregate_room_engagements
join_likes_to_rooms >> aggregate_room_engagements

# user rec flow
aggregate_user_engagements >> get_closest_users 
get_closest_users >> get_second_degree_users
[parse_follow_activities, get_second_degree_users] >> drop_invalid_sec_deg_users
drop_invalid_sec_deg_users >> [get_user_recommendations, get_user_rec_reasons]
get_user_recommendations >> load_user_recs_to_postgres
get_user_rec_reasons >> load_user_rec_reasons_to_postgres

# room rec flow
aggregate_room_engagements >> get_closest_rooms
[get_closest_users, get_closest_rooms] >> get_second_degree_rooms
[parse_subscribe_activities, get_second_degree_rooms] >> drop_invalid_sec_deg_rooms
drop_invalid_sec_deg_rooms >> [get_room_recommendations, get_room_rec_reasons]
get_room_recommendations >> load_room_recs_to_postgres
get_room_rec_reasons >> load_room_rec_reasons_to_postgres

# user metadata flow
get_user_indicator_columns >> convert_users_to_tsv >> load_users_to_postgres

# rooms metadata flow
get_room_indicator_columns >> convert_rooms_to_tsv >> load_rooms_to_postgres

# data quality checks
[
    load_user_recs_to_postgres,
    load_user_rec_reasons_to_postgres,
    load_room_recs_to_postgres,
    load_room_rec_reasons_to_postgres,
    load_users_to_postgres,
    load_rooms_to_postgres
] >> run_quality_checks
run_quality_checks >> end_operator

