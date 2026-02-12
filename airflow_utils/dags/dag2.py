from airflow.decorators import dag
from datetime import datetime
import sys
import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

sys.path.append('/opt/airflow/operators')
from twitteroperator import TwitterOperator

@dag(
    dag_id='twitter_pipeline_2',
    start_date= pendulum.datetime(2026, 2, 1),
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['twitter']
)
def pipeline_twitter():
    
    extrair_tweets = TwitterOperator(
        task_id='extrair_tweets',
        query='data science',
        file_path='/opt/data_lake/twitter_posts_raw/tweets_{{ ds }}.json'
    )
    twitter_transform = SparkSubmitOperator(task_id="transform_twitter_datascience", 
        application="/opt/airflow/src/spark/transformation.py",
        name="twitter_transformation",
        application_args=[
            "--src", "/opt/data_lake/twitter_posts_raw",
            "--dest", "/opt/airflow/data/silver"
        ])

    
    extrair_tweets >> twitter_transform

dag = pipeline_twitter()