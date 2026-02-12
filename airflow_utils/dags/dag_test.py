from airflow.decorators import dag
from datetime import datetime
import sys
import pendulum

sys.path.append('/opt/airflow/operators')
from twitteroperator import TwitterOperator

@dag(
    dag_id='twitter_pipeline',
    start_date= pendulum.datetime(2026, 2, 1),
    schedule='@daily',
    catchup=True,
    max_active_runs=1,
    tags=['twitter']
)
def pipeline_twitter():
    
    extrair_tweets = TwitterOperator(
        task_id='extrair_tweets',
        query='data science',
        file_path='/opt/data_hook/tweets_{{ ds }}.json'
    )

pipeline_twitter()