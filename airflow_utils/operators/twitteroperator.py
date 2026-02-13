import sys
sys.path.append('/opt/airflow/hook')

from airflow.models import BaseOperator
import json
from pathlib import Path
from twitterhook import TwitterHook


class TwitterOperator(BaseOperator):
    """
    Custom Airflow operator to fetch tweets using TwitterHook and save them as JSON lines.
    """
    template_fields = ['file_path', 'query']
    
    
    def __init__(self, query, file_path, **kwargs): 
        """
        Initialize the TwitterOperator with query and output file path.
        """
        self.query = query
        self.file_path = file_path
        super().__init__(**kwargs)
        
        
    def execute(self, context):
        """
        Execute the operator: fetch tweets for the DAG interval and save to file.
        """
        start_time = context['data_interval_start']
        end_time = context['data_interval_end']
        
        Path(self.file_path).parent.mkdir(parents=True, exist_ok=True)
        # Instantiate the hook and retrieve the tweets
        hook = TwitterHook(
            end_time=end_time,
            start_time=start_time,
            query=self.query
        )
        
        # Saves each tweet to the archive
        with open(self.file_path, 'w') as file:
            for page in hook.run():
                tweets = page.get('data', [])
                for tweet in tweets:
                    json.dump(tweet, file, ensure_ascii=False)
                    file.write("\n")
        self.log.info(f"Tweets salvos em {self.file_path}")
        
        return self.file_path