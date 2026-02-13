import sys
sys.path.append("/opt/airflow")  
from src.to_sql import gold_to_sql
import pendulum
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

sys.path.append('/opt/airflow/operators')
from twitteroperator import TwitterOperator


@dag(
    dag_id='tweets_pipeline',
    start_date= pendulum.datetime(2026, 2, 1),
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['twitter']
)
def pipeline_twitter():
    
    extract_tweets = TwitterOperator(
        task_id='extrair_tweets',
        query='data science',
        file_path='/opt/airflow/data/raw/tweets_{{ ds }}.json'
    )
    twitter_transform = SparkSubmitOperator(task_id="transform_twitter_datascience", 
        application="/opt/airflow/src/transformation.py",
        name="twitter_transformation",
        application_args=[
            "--src", "/opt/airflow/data/raw",
            "--dest", "/opt/airflow/data/silver"
        ])
    twitter_gold = SparkSubmitOperator(
    task_id="transform_twitter_gold", 
    application="/opt/airflow/src/transforming_gold.py",
    name="twitter_gold_transformation",
    application_args=[
        "--src", "/opt/airflow/data/silver",
        "--dest", "/opt/airflow/data/gold"
    ],
    verbose=True)
    
    gold_to_SQL = PythonOperator(
        task_id="gold_to_SQL",
        python_callable=gold_to_sql  
    )
    

    
    extract_tweets >> twitter_transform >> twitter_gold >> gold_to_SQL
    
    
dag = pipeline_twitter()