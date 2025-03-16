import json;
import boto3;
import pandas as pd;
from io import StringIO;
from datetime import datetime;

def github_transformation(response):
    data_list = [];

    for data in response:
        data_dict = {
            'name' : data['commit']['author']['name'],
            'email' : data['commit']['author']['email'].split('+')[-1],
            'date' : data['commit']['author']['date'],
            'time' : data['commit']['author']['date'],
        }
        data_list.append(data_dict);
    return data_list;



def lambda_handler(event, context):
    client = boto3.client('s3');
    bucket = 'snowflake-db-tutorial-ziggy';
    key = 'github_data/raw_data/to_processed/';

    github_data = [];
    github_key = [];

    for file in client.list_objects(Bucket=bucket, Prefix=key)['Contents']:
        file_key = file['Key'];
        if file_key.endswith('.json'):
            github_key.append(file_key);
            response = client.get_object(Bucket=bucket, Key=file_key);
            json_data = json.loads(response['Body'].read().decode('utf-8'));
            github_data.append(json_data);
    
    for data in github_data:
        data_list = github_transformation(data);
    
        data_df = pd.DataFrame(data_list);
        data_df['date'] = pd.to_datetime(data_df['date']).dt.strftime('%Y-%m-%d');
        data_df['time'] = pd.to_datetime(data_df['time']).dt.strftime('%H:%M:%S');
        data_df['date'] = pd.to_datetime(data_df['date']);

        github_file_key = 'github_data/transformed_data/github_transformed/transformed_data_' + str(datetime.now()) + '.csv';

        github_buffer = StringIO();
        data_df.to_csv(github_buffer, index=False);
        github_content = github_buffer.getvalue();
        client.put_object(Bucket=bucket, Key=github_file_key, Body=github_content);
    
    s3_resource = boto3.resource('s3');
    for final_key in github_key:
        copy_source = {
            'Bucket': bucket,
            'Key': final_key
        }
        s3_resource.meta.client.copy(copy_source, bucket, 'github_data/raw_data/processed_data/' + final_key.split('/')[-1]);
        s3_resource.Object(bucket, final_key).delete();