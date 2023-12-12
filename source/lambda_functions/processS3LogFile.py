import json
import boto3
import base64
import urllib.parse
from io import TextIOWrapper
import os
import re
import requests
from datetime import timezone
from datetime import datetime
import time
# import datetime
from aws_requests_auth.aws_auth import AWSRequestsAuth

ssm_client = boto3.client('ssm')

host = ssm_client.get_parameter(
    Name="s3auditor-opensearch-domain")['Parameter']['Value']
region = ssm_client.get_parameter(
    Name="s3auditor-opensearch-region")['Parameter']['Value']
service = 'opensearchservice'
s3_objects_index = 's3-objects'
s3_prefix_index = 's3-prefixes'
s3_activity_index = 's3-activity'
credentials = boto3.Session().get_credentials()

auth = AWSRequestsAuth(aws_access_key=os.environ['AWS_ACCESS_KEY_ID'],
                       aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                       aws_token=os.environ['AWS_SESSION_TOKEN'],
                       aws_host=host,
                       aws_region=region,
                       aws_service='es')

s3_line_logpats = r'(\S+) (\S+) \[(.*?)\] (\S+) (\S+) ' \
    r'(\S+) (\S+) (\S+) "([^"]+)" ' \
    r'(\S+) (\S+) (\S+) (\S+) (\S+) (\S+) ' \
    r'"([^"]+)" "([^"]+)"'

s3_line_logpat = re.compile(s3_line_logpats)

(S3_LOG_BUCKET_OWNER, S3_LOG_BUCKET, S3_LOG_DATETIME, S3_LOG_IP,
 S3_LOG_REQUESTOR_ID, S3_LOG_REQUEST_ID, S3_LOG_OPERATION, S3_LOG_KEY,
 S3_LOG_HTTP_METHOD_URI_PROTO, S3_LOG_HTTP_STATUS, S3_LOG_S3_ERROR,
 S3_LOG_BYTES_SENT, S3_LOG_OBJECT_SIZE, S3_LOG_TOTAL_TIME,
 S3_LOG_TURN_AROUND_TIME, S3_LOG_REFERER, S3_LOG_USER_AGENT) = range(17)

s3_names = ("bucket_owner", "bucket", "datetime", "ip", "requestor_id",
            "request_id", "operation", "key", "http_method_uri_proto", "http_status",
            "s3_error", "bytes_sent", "object_size", "total_time", "turn_around_time",
            "referer", "user_agent")

s3_client = boto3.client('s3')
os_client = boto3.client('opensearch')

headers = {"Content-Type": "application/json"}


def lambda_handler(event, context):

    # return {
    #     'statusCode': 200,
    #     'body': 'hi'
    # }

    for record in event['Records']:
        sns_message_body = json.loads(record['body'])
        body = json.loads(sns_message_body['Message'])

        source_bucket = body['detail']['bucket']['name']
        key = urllib.parse.unquote_plus(
            body['detail']['object']['key'], encoding='utf-8')

        # source_bucket = event['Records'][0]['s3']['bucket']['name']
        # key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

        print('Bucket: ' + source_bucket)
        print('LOG FILE: ' + key)

        result = s3_client.get_object(Bucket=source_bucket, Key=key)

        for line in result["Body"].read().splitlines():
            each_line = line.decode('utf-8')
            log_item = parse_s3_log_line(each_line)
            write_to_os_tsdb(log_item)


def write_to_os_tsdb(log_item):
    request_type = log_item[8]
    if ('GET' in request_type):
        bucket = log_item[1]
        key = log_item[7]
        timestamp_string = log_item[2]

        print(log_item[3])
        print(log_item[4])
        print(log_item[15])
        print(log_item[16])

        # 2022-04-28 15:51:23.455216+00:00
        # convert timestamp to above format from 05/Apr/2022:23:41:03 +0000
        timestamp_string = timestamp_string.replace(' +0000', '')
        datetime_object = datetime.strptime(
            timestamp_string, '%d/%b/%Y:%H:%M:%S')
        timestamp = datetime_object.strftime("%Y-%m-%d %H:%M:%S")

        # print('bucket: ' + bucket)
        # print('key: ' + key)

        update_bucket_prefix(bucket, key, timestamp)
        update_s3_object(bucket, key, timestamp)


def update_bucket_prefix(object_bucket, object_key, read_timestamp):
    items = object_key.split('/')
    items = items[:-1]  # remove the object name
    parent = ''

    # update bucket record itself
    message_bytes = object_bucket.encode('ascii')
    os_key = base64.b64encode(message_bytes)

    query = {"doc_as_upsert": 'true', "doc": {
        "last_read": str(read_timestamp)}}
    url = 'https://' + host + '/' + s3_prefix_index + '/_update/' + str(os_key)
    result = requests.post(url, headers=headers,
                           data=json.dumps(query), auth=auth, timeout=20)

    for item in items:
        if (len(parent)):
            id_string = object_bucket + '/' + parent + '/' + item
        else:
            id_string = object_bucket + '/' + item
        message_bytes = id_string.encode('ascii')
        os_key = base64.b64encode(message_bytes)

        query = {"doc_as_upsert": 'true', "doc": {
            "last_read": str(read_timestamp)}}

        # print('PREFIX_IDSTRING: ' + id_string)

        url = 'https://' + host + '/' + \
            s3_prefix_index + '/_update/' + str(os_key)
        result = requests.post(url, headers=headers,
                               data=json.dumps(query), auth=auth, timeout=20)
        # print(result.content)

        if (parent == ''):
            parent = item
        else:
            parent += '/' + item


def update_s3_object(object_bucket, object_key, read_timestamp):
    id_string = object_bucket + '/' + object_key
    message_bytes = id_string.encode('ascii')
    os_key = base64.b64encode(message_bytes)

    # print('object key: ' + object_key)
    # print('object bucket: ' + object_bucket)
    # print('key: ' + str(os_key))

    query = {"doc_as_upsert": 'true',
             "doc": {
                 "last_read": str(read_timestamp)
             }
             }

    url = 'https://' + host + '/' + \
        s3_objects_index + '/_update/' + str(os_key)
    result = requests.post(url, headers=headers,
                           data=json.dumps(query), auth=auth, timeout=20)
    # print(result.content)

    query = {
        "bucket": object_bucket,
        "key": object_key,
        "last_read": str(read_timestamp)
    }

    url = 'https://' + host + '/' + \
        s3_activity_index + '/_doc/' + str(time.time())
    result = requests.post(url, headers=headers,
                           data=json.dumps(query), auth=auth, timeout=20)
    # print('finished activity post')
    # print(result.content)


def parse_s3_log_line(line):
    match = s3_line_logpat.match(line)
    result = [match.group(1+n) for n in range(17)]
    return result
