import json
import boto3
import requests
import os
#import cfnresponse
from aws_requests_auth.aws_auth import AWSRequestsAuth

ssm_client = boto3.client('ssm')

host = ssm_client.get_parameter(Name="s3auditor-opensearch-domain")['Parameter']['Value']
region = ssm_client.get_parameter(Name="s3auditor-opensearch-region")['Parameter']['Value']
service = 'opensearchservice'

credentials = boto3.Session().get_credentials()
auth = AWSRequestsAuth(aws_access_key=os.environ['AWS_ACCESS_KEY_ID'],
                       aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                       aws_token=os.environ['AWS_SESSION_TOKEN'],
                       aws_host=host,
                       aws_region=region,
                       aws_service='es')
                       
headers = { "Content-Type": "application/json" }

def lambda_handler(event, context):
    
    url = 'https://' + host + '/s3-activity-test2'
    s3activity_index = {"settings":{"analysis": {"analyzer": {"whitespace_lowercase": {"tokenizer": "whitespace","filter": [ "lowercase" ]}}}},"mappings":{"properties":{"bucket": {"type":"keyword"},"key": {"type":"keyword"},"prefix": {"type":"keyword" },"access_details": {"type":"text" },"last_read": {"type":"date","format": "yyyy-MM-dd HH:mm:ss"}}}}
    result = requests.put(url, headers=headers, data=json.dumps(s3activity_index), auth=auth, timeout=20)
    print(result)

    url = 'https://' + host + '/s3-prefixes-test2'
    s3prefix_index = {"settings":{"analysis": {"filter": {"autocomplete_filter": {"type": "edge_ngram","min_gram": 1,"max_gram": 20}},"analyzer": {"whitespace_lowercase": {"tokenizer": "whitespace","filter": [ "lowercase" ]},"autocomplete": { "type": "custom","tokenizer": "standard","filter": ["lowercase","autocomplete_filter"]}}}},"mappings":{ "properties":{ "bucket": { "type":"keyword" }, "parent": { "type":"keyword" }, "prefix": { "type":"keyword"}, "aws_account": { "type":"keyword" }, "region": { "type":"keyword" }, "last_read": { "type":"date", "format": "yyyy-MM-dd HH:mm:ss", "null_value": "NULL" }, "last_write": { "type":"date", "format": "yyyy-MM-dd HH:mm:ss", "null_value": "NULL" }, "size": { "type":"long" }, "tags": {"type": "nested"},"search_field": {"type": "text","analyzer": "autocomplete", "search_analyzer": "standard" }} }}
    result = requests.put(url, headers=headers, data=json.dumps(s3prefix_index), auth=auth, timeout=20)
    print(result)
    
    url = 'https://' + host + '/s3-objects-test2'
    s3object_index = {"settings":{"analysis": {"filter": {"autocomplete_filter": {"type": "edge_ngram","min_gram": 1,"max_gram": 20}},"analyzer": {"whitespace_lowercase": {"tokenizer": "whitespace","filter": [ "lowercase" ]},"autocomplete": { "type": "custom","tokenizer": "standard","filter": ["lowercase","autocomplete_filter"]}}}},"mappings":{ "properties":{"bucket": { "type":"text", "fielddata": True, "analyzer":"whitespace_lowercase" }, "parent": { "type":"keyword" }, "prefix": { "type":"keyword" }, "storage_class": { "type":"keyword"  }, "aws_account": { "type":"keyword" }, "region": { "type":"keyword" }, "etag": { "type":"keyword" }, "deleted": {"type":"boolean","null_value": False, }, "object_name": { "type":"text", "fielddata": True, "analyzer":"whitespace_lowercase"  }, "last_read": { "type":"date", "format": "yyyy-MM-dd HH:mm:ss", "null_value": "NULL" }, "last_write": { "type":"date", "format": "yyyy-MM-dd HH:mm:ss", "null_value": "NULL" }, "size": { "type":"long" }, "tags": {"type": "nested"},"search_field": {"type": "text","analyzer": "autocomplete", "search_analyzer": "standard" }} }}
    result = requests.put(url, headers=headers, data=json.dumps(s3object_index), auth=auth, timeout=20)
    print(result)
    
    #cfnresponse.send(event, context, cfnresponse.SUCCESS, {"success":True}, "invokeOpensearchIndexCreation")
    
    return True