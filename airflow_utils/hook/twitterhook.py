from airflow.providers.http.hooks.http import HttpHook
import requests

class TwitterHook(HttpHook):
    
    
    def __init__(self,  end_time, start_time, query, http_conn_id="twitter_default"):
        """
        Initialize the Twitter hook with start and end times, query, and Airflow connection.
        """
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        super().__init__( http_conn_id=http_conn_id)
        
        
        
    def create_url(self):
        """
        Format start and end time, then build the Twitter API URL.
        """
        TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
        end_time = self.end_time.strftime(TIMESTAMP_FORMAT)
        start_time = self.start_time.strftime(TIMESTAMP_FORMAT)
        query  = self.query

        tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
        user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

        url_raw = f"{self.base_url}/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}"
        
        return url_raw
    
    
    
    def connect_to_endpoint(self, url_raw, session):
        """
        Send a GET request to the given URL using the provided session and return the response.
        """
        request = requests.Request("GET", url_raw)
        prep = session.prepare_request(request)
        self.log.info(f"Requesting {prep.url}")
    
        return self.run_and_check(session, prep, {})
        
        
        
    def paginate(self, url_raw, session):
        """
        Fetch multiple pages of results from the API, handling pagination via 'next_token'.
        """
        list_json_response = []
        response = self.connect_to_endpoint(url_raw, session)
        json_response = response.json()
        list_json_response.append(json_response)
        
        contador = 0
        
        while "next_token" in json_response.get("meta", {} )and contador<99:
            next_token = json_response["meta"]["next_token"]
            url_paginated = f"{url_raw}&next_token={next_token}"
            response = self.connect_to_endpoint(url_paginated, session)
            json_response = response.json()
            list_json_response.append(json_response)
            contador += 1
        
        return list_json_response
    
    
    
    def run(self):
        session = self.get_conn()
        url_raw = self.create_url()
        
        return self.paginate(url_raw, session)
