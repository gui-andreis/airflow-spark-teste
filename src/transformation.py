
from pyspark.sql.functions import col
from pyspark.sql.functions import to_date
from pyspark.sql.functions import current_date
from pyspark.sql import SparkSession
import argparse 

def get_tweets_data(df):
    """
    Extracting elements from within the array public.metrics and transforming them into columns.
    """   
    tweet_df_raw = df.select(
        "*",
        col("public_metrics.like_count").alias("like_count"),
        col("public_metrics.quote_count").alias("quote_count"),
        col("public_metrics.reply_count").alias("reply_count"),
        col("public_metrics.retweet_count").alias("retweet_count")
    ).drop("public_metrics")
    
    return tweet_df_raw


def adjusting_columns(tweet_df_raw):
    """   
    Renaming the columns, and creating two more.
    """
    new_cols = [
    "user_id",
    "thread_id",
    "created_at",
    "edit_versions_ids",
    "tweet_id",
    "reply_to_user_id",
    "language",
    "tweet_text",
    "likes",
    "quotes",
    "replies",
    "retweets"
]

    tweet_df= tweet_df_raw.toDF(*new_cols)
    tweet_df = tweet_df.withColumn("created_date", to_date("created_at"))
    tweet_df_clean = tweet_df.withColumn("processing_date", current_date())
    
    return tweet_df_clean
    
    
def load_parquet(tweet_df_clean, dest):
    """
    Saving file in parquet
    """
    tweet_df_clean.write.mode("overwrite") \
    .partitionBy("created_date") \
    .parquet(dest)
    
    
def run(spark,  dest, src):
    df = spark.read.json(src)
    tweet_df_raw = get_tweets_data(df)
    tweet_df_clean = adjusting_columns(tweet_df_raw)
    
    load_parquet(tweet_df_clean, dest)
    
    
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(
        description="Spark Twitter Transformation"
    )
    
    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)

    
    args = parser.parse_args()
    
    
    spark = SparkSession\
        .builder\
        .appName("twitter_transformation")\
        .getOrCreate()
    
    run(spark = spark, src = args.src, dest= args.dest)
        