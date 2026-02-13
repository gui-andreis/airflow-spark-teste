from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size,  count, avg
import argparse 

def add_metrics(df):
    """
    Add the 'engagement' column, summing likes, retweets, replies, and quotes.
    """

    df = df.withColumn(
        "engagement",
        col("likes") + col("retweets") + col("replies") + col("quotes")
    )
    df= df.withColumn("num_edits", size(col("edit_versions_ids")))
    
    return df
    

def add_avg_eng_per_day(df):
    """
    Create the column avd_eng_per_day and join it to the main table.
    """
    avg_eng = df.groupBy("created_date").agg(avg("engagement").alias("avg_eng_per_day"))
    
    return df.join(avg_eng, on="created_date", how="left")


def add_user_metrics(df):
    """ 
    Create the column avd_eng_per_day and join it to the main table.
    """
    user_metrics = df.groupBy("user_id").agg(
        count("*").alias("total_tweets"),
        avg("engagement").alias("avg_eng_per_user"))
    
    return df.join(user_metrics, on="user_id", how="left")


def load_parquet(df, dest):
    """ 
    Saving file in parquet 
    """
    df.write.mode("overwrite") \
    .partitionBy("created_date") \
    .parquet(dest)
    

def build_gold(spark, src, dest):
    
    df = spark.read.parquet(src)
    df = add_metrics(df)
    df = add_avg_eng_per_day(df)
    df = add_user_metrics(df)
    gold_df = df.select(
        "user_id", "thread_id", "tweet_id", "created_at", "created_date",
    "tweet_text","likes", "quotes", "replies", "retweets", "engagement", 
    "avg_eng_per_day", "total_tweets", "avg_eng_per_user",
    "language")
    load_parquet(gold_df, dest)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("TwitterGold").getOrCreate()
    
    build_gold(spark, args.src, args.dest)



