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

index = 0
parents = []


def lambda_handler(event, context):

    global index
    # first get all accounts and then go down for each account and prefix to build a navigation tree
    query = {"aggs": {"distinct_accounts": {"terms": {"field": "aws_account"}}}}

    results = run_query(query)
    db = []
    for act_info in results['aggregations']['distinct_accounts']['buckets']:
        account = {'key': index, 'icon': 'fa fa-folder',
                   'title': act_info['key'], 'label': act_info['key'], 'path': '', 'children': []}
        index += 1
        query = {"query": {"term": {"aws_account": {"value": act_info['key'], "boost": 1.0}}}, "aggs": {
            "distinct_buckets": {"terms": {"field": "bucket"}}}}
        buckets = run_query(query)
        for bucket_info in buckets['aggregations']['distinct_buckets']['buckets']:
            path = ''
            child = {
                'key': index, 'icon': 'fa fa-folder', 'title': bucket_info['key'], 'bucket': bucket_info['key'], 'label': bucket_info['key'], 'path': path, 'children': getBucketPrefixes(bucket_info['key'], '', path)
            }

            account['children'].append(child)
            index += 1

        db.append(account)

    return db


def getBucketPrefixes(bucket, parent, path):
    global index

    index += 1

    query = {"query": {"bool": {"must": [{"term": {"bucket": bucket}}, {"term": {"parent": parent}}], "must_not": [
        {"term": {"prefix": ""}}]}}, "aggs": {"distinct_prefixes": {"terms": {"field": "prefix"}}}}
    prefixes = run_query(query)

    obj = []

    for prefix in prefixes['aggregations']['distinct_prefixes']['buckets']:

        new_path = path + prefix['key'] + '/'

        new_parent = parent
        if (len(new_parent)):
            new_parent += '/'
        new_parent += prefix['key']

        obj.append({
            'key': index, 'icon': 'fa fa-folder', 'bucket': bucket, 'title': prefix['key'], 'label': prefix['key'], 'path': new_path, 'children': getBucketPrefixes(bucket, new_parent, new_path)
        })

    return obj


def run_query(query):
    url = 'https://' + host + '/s3-prefixes/_search/'
    result = requests.get(url, headers=headers,
                          data=json.dumps(query), auth=auth, timeout=20)
    obj = json.loads(result.text)

    return obj
