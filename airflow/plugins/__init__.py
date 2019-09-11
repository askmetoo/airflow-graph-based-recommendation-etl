from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class SocialRecEtlPlugin(AirflowPlugin):
    name = "social_rec_etl_plugin"
    operators = [
        operators.GetIndicatorColumnsOperator,
        operators.CsvToTsvOperator,
        operators.LoadS3ToPostgresOperator,
        operators.ParseActivityJsonOperator,
        operators.UnstackColumnOperator,
        operators.DropColumnOperator,
        operators.JoinActivityDataOperator,
        operators.AggregateEngagementsOperator,
        operators.GetStrongestConnectionsOperator,
        operators.GetSecDegConnectionsOperator,
        operators.DropInvalidConnectionsOperator,
        operators.GetGraphRecommendationsOperator,
        operators.GetGraphRecReasonsOperator,
        operators.DataQualityOperator
    ]
    helpers = [
        helpers.DataHelper,
        helpers.RecommendationHelper
    ]
