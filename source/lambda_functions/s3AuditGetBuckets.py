import json
import boto3
import requests
import base64
import urllib.parse
import os
from aws_requests_auth.aws_auth import AWSRequestsAuth

ssm_client = boto3.client('ssm')

host = ssm_client.get_parameter(
    Name="s3auditor-opensearch-domain")['Parameter']['Value']
region = ssm_client.get_parameter(
    Name="s3auditor-opensearch-region")['Parameter']['Value']
service = 'opensearchservice'

credentials = boto3.Session().get_credentials()
auth = AWSRequestsAuth(aws_access_key=os.environ['AWS_ACCESS_KEY_ID'],
                       aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                       aws_token=os.environ['AWS_SESSION_TOKEN'],
                       aws_host=host,
                       aws_region=region,
                       aws_service='es')

headers = {"Content-Type": "application/json"}


def lambda_handler(event, context):

    query = {
        "aggs": {
            "distinct_buckets": {
                "terms": {
                    "field": "bucket"
                }
            }
        }
    }

    url = 'https://' + host + '/s3-prefixes/_search/'
    result = requests.get(url, headers=headers,
                          data=json.dumps(query), auth=auth, timeout=20)
    obj = json.loads(result.text)
    db = []
    for bucket_info in obj['aggregations']['distinct_buckets']['buckets']:
        db.append(bucket_info['key'])

    db.sort()

    return {
        'statusCode': 200,
        'body': json.dumps(db)
    }
