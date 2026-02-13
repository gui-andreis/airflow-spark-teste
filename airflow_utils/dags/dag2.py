import sys
sys.path.append("/opt/airflow")  
from src.spark.to_sql import gold_to_sql

sys.path.append('/opt/airflow/operators')

from airflow.decorators import dag
import sys
import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator




from twitteroperator import TwitterOperator

@dag(
    dag_id='twitter_pipeline_2',
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
        file_path='/opt/airflow/data/raw/tweets_{{ ds }}.json'
    )
    twitter_transform = SparkSubmitOperator(task_id="transform_twitter_datascience", 
        application="/opt/airflow/src/spark/transformation.py",
        name="twitter_transformation",
        application_args=[
            "--src", "/opt/airflow/data/raw",
            "--dest", "/opt/airflow/data/silver"
        ])
    twitter_gold = SparkSubmitOperator(
    task_id="transform_twitter_gold", 
    application="/opt/airflow/src/spark/transforming_gold.py",
    name="twitter_gold_transformation",
    application_args=[
        "--src", "/opt/airflow/data/silver",
        "--dest", "/opt/airflow/data/gold"
    ],
    verbose=True)
    
    gold_to_SQL = PythonOperator(
        task_id="gold_to_SQL",
        python_callable=gold_to_sql  # roda a função main do seu to_sql.py
    )
    

    
    extrair_tweets >> twitter_transform >> twitter_gold >> gold_to_SQL
dag = pipeline_twitter()