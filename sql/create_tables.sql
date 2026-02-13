CREATE TABLE IF NOT EXISTS gold_twitter (
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