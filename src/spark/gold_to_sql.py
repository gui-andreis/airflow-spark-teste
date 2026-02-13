from pyspark.sql import SparkSession
import argparse

def run(src):
    spark = SparkSession.builder.appName("GoldToPostgres").getOrCreate()
    
    # Lê a Gold
    df = spark.read.parquet(src)

    # Grava no Postgres na tabela de STAGING (estágio)
    # O mode("overwrite") garante que a tabela temporária sempre tenha apenas o dado atual
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://spark-postgres:5432/gold_database_tweets") \
        .option("dbtable", "gold_tweets_table") \
        .option("user", "teste") \
        .option("password", "12345") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--src")
    args = parser.parse_args() 
    run(args.src)