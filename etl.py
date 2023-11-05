# import libraries
from util import generate_schema, get_redshift_connection,\
execute_sql, list_files_in_folder
import pandas as pd
import requests
import boto3
from datetime import datetime
from io import StringIO
import io
import psycopg2
import ast
from dotenv import dotenv_values
dotenv_values()

# Get credentials from environment variable file
config = dotenv_values('.env')

# Create a boto3 s3 client for bucket operations
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

def get_data_from_api():
    url = config.get('URL')
    headers = config.get('HEADERS')
    querystring = config.get('QUERYSTRING')
    try:
        # Send request to Rapid API and return the response as a Json object
        response = requests.get(url, headers=headers, params=querystring).text
    except ConnectionError:
        print('Unable to connect to the URL endpoint')
    coin_data = response.get('data').get('coins')
    columns = ['symbol', 'name', 'price', 'rank']
    crypto_price_data = pd.DataFrame(coin_data)[columns]
    return crypto_price_data


def read_from_azure_blob_storage(bucket_name, path):
    objects_list = s3_client.blob_list(Bucket = bucket_name, Prefix = path) # List the objects in the bucket
    file = objects_list.get('Contents')[1]
    key = file.get('Key') # Get file path or key
    obj = s3_client.get_object(Bucket = bucket_name, Key= key)
    data = pd.read_csv(io.BytesIO(obj['Body'].read()))
    return data

def write_to_azure_blob_storage(data, bucket_name, folder):
    file_name = f"crypto_price_data_{datetime.now().strftime('%Y%m%d')}.csv" # Create a file name
    csv_buffer = StringIO() # Create a string buffer to collect csv string
    data.to_csv(csv_buffer, index=False) # Convert dataframe to CSV file and add to buffer
    # using the put_object(write) operation to write the data into s3
    s3_client.put_object(Bucket=bucket_name, Key=f'{folder}/{file_name}', Body=csv_str ) 
    return csv_buffer

