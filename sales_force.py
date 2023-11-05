import requests
import pandas as pd
from io import StringIO
import io
from datetime import datetime
import boto3
from dotenv import dotenv_values
dotenv_values()


# Instantiate an S3 client object
s3_client = boto3.client('s3')

'''
Writing data to AWS S3 Bucket

When building out an ETL pipeline, we often need to temporarily stage extracted or transformed
files in a data lake. One of the important reason for this is so we can have a backup of historical 
data outside the data warehouse so we can easily reload the data when things go wrong in the warehouse.
In this section we will work on a mini project to extract and write the data to an Amazon S3 bucket.
'''
# AWS resources
bucket_name = '10alytics-api-data'
path = 'log_data' # Specify foler/path


# Reading a single file from S3 Bucket
def read_from_s3():
    bucket_name = 'bucketname'
    path = 'log_data' # Specify foler/path
    objects_list = s3_client.list_objects(Bucket = bucket_name, Prefix = path) # List the objects in the bucket
    file = objects_list.get('Contents')[1]
    key = file.get('Key') # Get file path or key
    obj = s3_client.get_object(Bucket = bucket_name, Key= key)
    data = pd.read_csv(io.BytesIO(obj['Body'].read()))
    return data


# Read multiple files from S3 bucket
def read_multi_files_from_s3():
    objects_list = s3_client.list_objects(Bucket = bucket_name, Prefix = path) # List the objects in the bucket
    files = objects_list.get('Contents')
    keys = [file.get('Key') for file in files][1:]
    objs = [s3_client.get_object(Bucket = bucket_name, Key= key) for key in keys]
    dfs = [pd.read_csv(io.BytesIO(obj['Body'].read())) for obj in objs]
    data = pd.concat(dfs)
    return data
#read_multi_files_from_s3()


# Read Local CSV file
def read_local_csv():
    csv_data = pd.read_csv('log_data4.csv')
    return csv_data


# Write data to S3 Bucket
def write_to_s3(data):
    bucket_name = '10alytics-api-data'
    path = 'test_data' # Specify foler/path
    file_name = f"{datetime.now().strftime('%Y-%m-%d-%H-%M')}" # Create a file name
    csv_buffer = StringIO() # Create a string buffer to collect csv string
    data.to_csv(csv_buffer, index=False) # Convert dataframe to CSV file and add to buffer
    csv_str = csv_buffer.getvalue() # Get the csv string
    # using the put_object(write) operation to write the data into s3
    s3_client.put_object(Bucket=bucket_name, Key=f'{path}/{file_name}', Body=csv_str ) 
    print('File successfully written to s3 bucket')


csv_data = read_local_csv()
write_to_s3(csv_data)