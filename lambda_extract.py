import json;
import requests;
import os;
import boto3;
from datetime import datetime;

def lambda_handler(event, context):
        url = 'https://api.github.com/repos/facebook/react/commits';
        token = os.environ['token'];
        headers = {'Authorization': token};
        response = requests.get(url, headers=headers).json();

        client = boto3.client('s3');

        client.put_object(
            Bucket='snowflake-db-tutorial-ziggy',
            Body=json.dumps(response),
            Key='github_data/raw_data/to_processed/github_raw_' + str(datetime.now()) + '.json'
        );
    
