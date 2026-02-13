
# Config DB
DB_HOST = "postgres-data"
DB_NAME = "twitter_gold"
DB_USER = "teste"
DB_PASSWORD = "12345"
DB_PORT = "5432"

tabela = "gold_twitter"

def gold_to_sql():
   
    
    import psycopg2
    import pandas as pd
    from psycopg2.extras import execute_values
    df = pd.read_parquet("/opt/airflow/data/gold")
    # Connect to Postgres
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )
    cursor = conn.cursor()

    #Create a table if it doesn't exist.
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {tabela} (
            user_id BIGINT,
            thread_id BIGINT,
            tweet_id BIGINT,
            created_at TIMESTAMP,
            created_date DATE,
            tweet_text TEXT,
            likes INT,
            quotes INT,
            replies INT,
            retweets INT,
            engagement FLOAT,
            avg_eng_per_day FLOAT,
            total_tweets INT,
            avg_eng_per_user FLOAT,
            language VARCHAR(10),
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()

    # Converts DataFrame into a list of tuples.
    registros = [
        (
            row["user_id"], row["thread_id"], row["tweet_id"], row["created_at"], row["created_date"],
            row["tweet_text"], row["likes"], row["quotes"], row["replies"], row["retweets"], row["engagement"],
            row["avg_eng_per_day"], row["total_tweets"], row["avg_eng_per_user"], row["language"]
        )
        for _, row in df.iterrows()
    ]

    # Insert the data into the database.
    query = f"""
        INSERT INTO {tabela} (
            user_id, thread_id, tweet_id, created_at, created_date, tweet_text,
            likes, quotes, replies, retweets, engagement, avg_eng_per_day,
            total_tweets, avg_eng_per_user, language
        )
        VALUES %s;
    """

    execute_values(cursor, query, registros)
    conn.commit()
    cursor.close()
    conn.close()

    print(f" - - Dados inseridos no Postgres na tabela: {tabela}")
