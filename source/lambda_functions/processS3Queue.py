import json
import boto3
import base64
import urllib.parse
from io import TextIOWrapper
import os
import requests
from datetime import timezone
import datetime
from aws_requests_auth.aws_auth import AWSRequestsAuth
import logging

ssm_client = boto3.client('ssm')

host = ssm_client.get_parameter(
    Name="s3auditor-opensearch-domain")['Parameter']['Value']
region = ssm_client.get_parameter(
    Name="s3auditor-opensearch-region")['Parameter']['Value']
service = 'opensearchservice'
s3_objects_index = 's3-objects'
s3_prefix_index = 's3-prefixes'
credentials = boto3.Session().get_credentials()

auth = AWSRequestsAuth(aws_access_key=os.environ['AWS_ACCESS_KEY_ID'],
                       aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                       aws_token=os.environ['AWS_SESSION_TOKEN'],
                       aws_host=host,
                       aws_region=region,
                       aws_service='es')

headers = {"Content-Type": "application/json"}


def lambda_handler(event, context):

    try:
        for record in event['Records']:
            sns_message_body = json.loads(record['body'])
            body = json.loads(sns_message_body['Message'])

            if (('reason' in body['detail'] and (body['detail']['reason'] == 'PutObject' or body['detail']['reason'] == 'CopyObject'))
                    or ('detail-type' in body and 'tags' in body['detail-type'].lower())):

                bucket_region = body['region']
                aws_account = body['account']
                event_time = body['time']

                source_bucket = body['detail']['bucket']['name']
                key = urllib.parse.unquote_plus(
                    body['detail']['object']['key'], encoding='utf-8')

                s3 = boto3.resource('s3', region_name=bucket_region)
                summary = s3.ObjectSummary(source_bucket, key)

                object_info = {
                    'object_bucket': source_bucket,
                    'object_key': key,
                    'object_size': summary.size,
                    'aws_account': aws_account,
                    'bucket_region': bucket_region,
                    'event_time': str(summary.last_modified)
                }

                if (int(summary.size) > 0):
                    save_s3_bucket_prefixes(object_info)
                    save_s3_object(object_info, summary)

            elif (body['detail']['reason'] == 'DeleteObject'):
                source_bucket = body['detail']['bucket']['name']
                key = urllib.parse.unquote_plus(
                    body['detail']['object']['key'], encoding='utf-8')

                object_info = {
                    'object_bucket': source_bucket,
                    'object_key': key
                }

                delete_s3_object(object_info)

    except Exception as e:
        print('ERROR IN MAIN BODY')
        print(str(e))
        print('Records')
        print(event['Records'])


def save_s3_bucket_prefixes(object_info):
    try:
        items = object_info['object_key'].split('/')
        items = items[:-1]  # remove the object name
        parent = ''

        last_write = datetime.datetime.strptime(
            object_info['event_time'], '%Y-%m-%d %H:%M:%S+00:00').strftime("%Y-%m-%d %H:%M:%S")

        # save write to bucket
        message_bytes = object_info['object_bucket'].encode('ascii')
        bucket_key = base64.b64encode(message_bytes)
        query = {"doc_as_upsert": 'true',
                 "doc": {
                     "bucket": object_info['object_bucket'],
                     "parent": '',
                     "prefix": '',
                     "region": object_info['bucket_region'],
                     "aws_account": object_info['aws_account'],
                     "last_write": last_write,
                     "search_field": object_info['object_bucket']
                 }
                 }

        url = 'https://' + host + '/' + \
            s3_prefix_index + '/_update/' + str(bucket_key)
        result = requests.post(url, headers=headers,
                               data=json.dumps(query), auth=auth, timeout=20)

        for item in items:
            if (len(parent)):
                id_string = object_info['object_bucket'] + \
                    '/' + parent + '/' + item
            else:
                id_string = object_info['object_bucket'] + '/' + item
            message_bytes = id_string.encode('ascii')
            os_key = base64.b64encode(message_bytes)

            query = {"doc_as_upsert": 'true',
                     "doc": {
                         "bucket": object_info['object_bucket'],
                         "parent": parent,
                         "prefix": item,
                         "region": object_info['bucket_region'],
                         "aws_account": object_info['aws_account'],
                         "last_write": last_write,
                         "search_field": object_info['object_bucket'] + ' ' + parent + ' ' + item
                     }
                     }

            url = 'https://' + host + '/' + \
                s3_prefix_index + '/_update/' + str(os_key)
            result = requests.post(url, headers=headers, data=json.dumps(
                query), auth=auth, timeout=20)

            if (parent == ''):
                parent = item
            else:
                parent += '/' + item

    except Exception as e:
        print('ERROR IN BUCKET PREFIX')
        print(e)


def save_s3_object(object_info, object_summary):
    try:
        id_string = object_info['object_bucket'] + \
            '/' + object_info['object_key']
        message_bytes = id_string.encode('ascii')
        os_key = base64.b64encode(message_bytes)

        last_write = datetime.datetime.strptime(
            object_info['event_time'], '%Y-%m-%d %H:%M:%S+00:00').strftime("%Y-%m-%d %H:%M:%S")

        storage_class = object_summary.storage_class
        if (storage_class is None):
            storage_class = 'STANDARD'

        e_tag = object_summary.e_tag
        if (e_tag is None):
            e_tag = ''
        else:
            e_tag = e_tag.replace('"', '')

        sse_encryption = object_summary.Object().server_side_encryption
        if (sse_encryption is None):
            sse_encryption = 'NO'
        if (sse_encryption == 'aws:kms'):
            sse_encryption = 'KMS'

        # etag differences https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html

        # get object tags
        s3_client = boto3.client(
            "s3", region_name=object_info['bucket_region'])
        s3_response = s3_client.get_object_tagging(
            Bucket=object_info['object_bucket'], Key=object_info['object_key'])
        metadata = s3_response.get("Tags")
        tags = []
        tag_vals = []
        for tag in s3_response['TagSet']:
            tags.append({tag['Key']: tag['Value']})
            tag_vals.append(tag['Value'])

        # get object name
        items = object_info['object_key'].split('/')
        object_name = items.pop()

        query = {"doc_as_upsert": 'true',
                 "doc": {
                     "bucket": object_info['object_bucket'],
                     "key": object_info['object_key'],
                     "size": object_info['object_size'],
                     "last_write": last_write,
                     "tags": tags,
                     "storage_class": storage_class,
                     "sse_encryption": sse_encryption,
                     "aws_account": object_info['aws_account'],
                     "prefix": "/".join(str(x) for x in items),
                     "object_name": object_name,
                     "region": object_info['bucket_region'],
                     "etag": e_tag,
                     "search_field": object_info['object_bucket'] + ' ' + object_info['object_key'] + ' ' + " ".join(str(x) for x in tag_vals)
                 }
                 }

        url = 'https://' + host + '/' + \
            s3_objects_index + '/_update/' + str(os_key)
        result = requests.post(url, headers=headers,
                               data=json.dumps(query), auth=auth, timeout=20)
        if (result.status_code >= 300):
            print("ERROR FROM OPENSEARCH")

            print("QUERY")
            print(query)

            print("RESULT")
            print(result)

    except Exception as e:
        print('ERROR IN SAVE S3 OBJECT')
        print(e)


def delete_s3_object(object_info):
    # objects are never delete from opensearch
    # only marked as deleted

    # modify this code if you'd like to permanently delete object from your index

    id_string = object_info['object_bucket'] + '/' + object_info['object_key']
    message_bytes = id_string.encode('ascii')
    os_key = base64.b64encode(message_bytes)

    query = {"doc_as_upsert": 'true',
             "doc": {
                 "deleted": True
             }
             }

    url = 'https://' + host + '/' + \
        s3_objects_index + '/_update/' + str(os_key)
    result = requests.post(url, headers=headers,
                           data=json.dumps(query), auth=auth, timeout=20)
