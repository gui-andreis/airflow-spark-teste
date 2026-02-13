from airflow.decorators import dag
import sys
import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


sys.path.append('/opt/airflow/operators')
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
    
    gold_to_stg = SparkSubmitOperator(
        task_id="gold_to_stg",
        application="/opt/airflow/src/spark/gold_to_sql.py",
        jars="/opt/airflow/drivers/postgresql-42.2.23.jar", 
        application_args=["--src", "/opt/airflow/data/gold"]
    )
    
    stg_to_final = PostgresOperator(
        task_id="stg_to_final",
        postgres_conn_id="postgres_default", # Configure essa conexão na UI do Airflow
        sql="""
            INSERT INTO tweets_final (user_id, tweet_id, engagement, created_date)
            SELECT user_id, tweet_id, engagement, created_date FROM stg_tweets
            ON CONFLICT (tweet_id) 
            DO UPDATE SET engagement = EXCLUDED.engagement;
        """
    )
    

    
    extrair_tweets >> twitter_transform >> twitter_gold

dag = pipeline_twitter()