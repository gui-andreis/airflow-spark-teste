from datetime import datetime, timedelta 
from dotenv import load_dotenv
import os
import requests
import pandas as pd
import json
# Carrega as variáveis do .env
load_dotenv()


#montando a url para a requisição
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"


end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
query  = "data science"


tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

url_raw = f"https://labdados.com/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}"

#montando header
bearer_token = os.getenv("BEARER_TOKEN")
headers ={"authorization": f"Bearer {bearer_token}"}

response = requests.get(url_raw, headers=headers, timeout=10)

json_response = response.json()

# print(json.dumps(json_response, indent=4))


# df_tweets = pd.DataFrame(json_response["data"])
# df_users = pd.DataFrame(json_response["includes"]["users"])

# print(df_tweets.head())
# print(df_users.head())
#paginate q é inútil vou deixar mas provavelmente eu tire 
while "next_token" in json_response.get("meta", {}):
    next_token = json_response["meta"]["next_token"]
    url_paginated = f"{url_raw}&next_token={next_token}"
    response = requests.get(url_paginated, headers=headers, timeout=10)
    json_response = response.json()
    print(json.dumps(json_response, indent=4))
    
    
    