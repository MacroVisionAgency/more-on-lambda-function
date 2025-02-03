import json
import logging
import os
import boto3
import pandas as pd
from dotenv import load_dotenv


def create_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    if not logger.handlers:  # Prevent duplicate handlers
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)

    return logger


def load_dotEnv():
    load_dotenv()
    access = os.getenv('access')
    secret = os.getenv('secret')
    bucket = os.getenv('S3_BUCKET_NAME')
    if not bucket:
        print("Warning: S3_BUCKET_NAME is not set in the .env file")
    return access, secret, bucket


def create_s3Client(logger):
    s3 = boto3.client('s3')
    logger.info('S3 client Created')
    return s3


def get_data(event):
    if 'queryStringParameters' not in event:
        return {
            'statusCode': 400,
            'body': json.dumps('No query string parameters provided')
        }
    query_params = event['queryStringParameters']
    required_fields = ['name', 'email', 'phone']
    if not all(field in query_params for field in required_fields):
        return {
            'statusCode': 400,
            'body': json.dumps('Missing required query parameters.')
        }

    return {
        'name': query_params['name'],
        'email': query_params['email'],
        'phone': query_params['phone']
    }


def create_csv(data, logger):
    df = pd.DataFrame([data])
    csv_data = df.to_csv(index=False)
    logger.info("CSV data created")
    return csv_data


def put_object(s3, bucket, data, csv_data, logger):
    key = f"uploaded_data/{data['name']}_data.csv"
    logger.info(f"Bucket name: {bucket}")
    s3.put_object(Bucket=bucket, Key=key, Body=csv_data)
    logger.info("CSV data uploaded to S3 bucket")
    return key


def get_object(s3, bucket, key, logger):
    obj = s3.get_object(Bucket=bucket, Key=key)
    obj_data = obj['Body'].read().decode('utf-8')
    logger.info("CSV data retrieved from S3 bucket")
    return obj_data


def lambda_handler(event, context):
    logger = create_logger()
    access, secret, bucket = load_dotEnv()
    s3 = create_s3Client(logger)
    data = get_data(event)
    if 'statusCode' in data:
        return data
    csv_data = create_csv(data, logger)
    key = put_object(s3, bucket, data, csv_data, logger)
    obj_data = get_object(s3, bucket, key, logger)
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "CSV uploaded and retrieved successfully",
            "file_content": obj_data
        })
    }


if __name__ == '__main__':
    lambda_handler({}, {})
