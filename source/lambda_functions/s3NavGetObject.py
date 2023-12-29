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

    # query string params from API gateway
    qs = event['params']['querystring']
    bucket = qs['b']
    key = qs['p']

    db = {
        'info': {},
        'activity': []
    }

    query = {
        "from": 0, "size": 1,
        "query": {"bool": {"must": [{"match": {"bucket": bucket}}, {"match": {"key": key}}]}},
        "_source": True
    }

    # return query

    results = run_objects_query(query)

    # return results
    result = results['hits']['hits'][0]['_source']

    db['info'] = {
        'account': result['aws_account'],
        'bucket': result['bucket'],
        'prefix': result['prefix'],
        'key': result['key'],
        'object_name': result['object_name'],
        'tags': result['tags'] if 'tags' in result else [],
        'storage_class': result['storage_class'] if 'storage_class' in result else '',
        'etag': result['etag'] if 'etag' in result else '',
        'sse_encryption': result['sse_encryption'] if 'sse_encryption' in result else '',
        'region': result['region'] if 'region' in result else '',
        'size': result['size'],
        'last_write': result['last_write'],
        'last_read': result['last_read'] if 'last_read' in result else '',
    }

    query = {
        "from": 0, "size": 10,
        "sort": [{"last_read": "desc"}],
        "query": {"bool": {"must": [{"match": {"bucket": bucket}}, {"match": {"key": key}}]}},
        "_source": True
    }

    # return query

    results = run_activity_query(query)
    for item in results['hits']['hits']:
        db['activity'].append(item['_source'])

    return db


def run_objects_query(query):
    url = 'https://' + host + '/s3-objects/_search/'
    result = requests.get(url, headers=headers,
                          data=json.dumps(query), auth=auth, timeout=20)
    obj = json.loads(result.text)

    return obj


def run_activity_query(query):
    url = 'https://' + host + '/s3-activity/_search/'
    result = requests.get(url, headers=headers,
                          data=json.dumps(query), auth=auth, timeout=20)
    obj = json.loads(result.text)

    return obj
