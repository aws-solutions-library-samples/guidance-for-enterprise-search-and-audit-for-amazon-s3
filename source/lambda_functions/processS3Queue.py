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

# boto3.set_stream_logger('')

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

    # s3_client = boto3.client("s3", region_name='us-east-1')
    # s3_response = s3_client.get_object(Bucket='detailed-s3-activity-1', Key='AnalystAnalystCandle_rvy.txt')
    # print('size: ' + str(s3_response.get('ServerSideEncryption')))
    # return {}

    # s3_client = boto3.client("s3", region_name='us-east-1')
    # s3_response = s3_client.get_object_tagging(Bucket='detailed-s3-activity-1', Key='AnalystAnalystCandle_rvy.txt')
    # metadata = s3_response.get("Tags")
    # for tag in s3_response['TagSet']:
    #     print(tag['Key'] + ' ' + tag['Value'])
    # return {}

    # s3 = boto3.resource('s3')
    # object_summary = s3.ObjectSummary('detailed-s3-activity-1','AnalystAnalystCandle_rvy.txt')
    # print(object_summary.Object().server_side_encryption)
    # return {}

    # s3 = boto3.resource('s3', region_name='eu-south-1')
    # object_summary = s3.ObjectSummary('test-cross-region-bucket-ilya','test_get.txt')
    # print(object_summary.storage_class)
    # print(object_summary.Object().server_side_encryption)
    # print(object_summary.Object().ssekms_key_id)
    # return {}

    # query = {
    #     "size": 25,
    #     "query": {
    #         "multi_match": {
    #             "query": '521195769131',
    #             "fields": ["aws_account"]
    #         }
    #     }
    # }
    # url = 'https://' + host + '/' + s3_prefix_index + '/_search'
    # r = requests.get(url, auth=auth, headers=headers, data=json.dumps(query))
    # print(r)

    try:
        for record in event['Records']:
            sns_message_body = json.loads(record['body'])
            body = json.loads(sns_message_body['Message'])

            # print('STARTING PROCESS')
            # print(body)
            # return True

            if (('reason' in body['detail'] and (body['detail']['reason'] == 'PutObject' or body['detail']['reason'] == 'CopyObject'))
                    or ('detail-type' in body and 'tags' in body['detail-type'].lower())):

                bucket_region = body['region']
                aws_account = body['account']
                event_time = body['time']

                source_bucket = body['detail']['bucket']['name']
                key = urllib.parse.unquote_plus(
                    body['detail']['object']['key'], encoding='utf-8')
                # size = body['detail']['object']['size']

                s3 = boto3.resource('s3', region_name=bucket_region)
                summary = s3.ObjectSummary(source_bucket, key)

                # print(source_bucket + '/' + key + ' - ' + str(size) + ' - ' + aws_account + ' - ' + bucket_region + ' - ' + str(event_time))

                object_info = {
                    'object_bucket': source_bucket,
                    'object_key': key,
                    'object_size': summary.size,
                    'aws_account': aws_account,
                    'bucket_region': bucket_region,
                    'event_time': str(summary.last_modified)
                }

                # print(object_info)

                if (int(summary.size) > 0):
                    save_s3_bucket_prefixes(object_info)
                    save_s3_object(object_info, summary)

            elif (body['detail']['reason'] == 'DeleteObject'):
                # print('DELETING OBJECT')

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

        # print('PREFIX')
        # print(query)

        url = 'https://' + host + '/' + \
            s3_prefix_index + '/_update/' + str(bucket_key)
        result = requests.post(url, headers=headers,
                               data=json.dumps(query), auth=auth, timeout=20)
        # print(result)

        for item in items:
            if (len(parent)):
                id_string = object_info['object_bucket'] + \
                    '/' + parent + '/' + item
            else:
                id_string = object_info['object_bucket'] + '/' + item
            message_bytes = id_string.encode('ascii')
            os_key = base64.b64encode(message_bytes)

            # print('PREFIX_IDSTRING: ' + id_string)

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

            # print('PREFIX')
            # print(query)

            url = 'https://' + host + '/' + \
                s3_prefix_index + '/_update/' + str(os_key)
            result = requests.post(url, headers=headers, data=json.dumps(
                query), auth=auth, timeout=20)
            # print(result)

            if (parent == ''):
                parent = item
            else:
                parent += '/' + item

            # print(parent)

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

        # print('OBJECT_IDSTRING: ' + id_string)

        # print('SAVING S3 OBJECT')
        # print(object_info['bucket_region'] + ' - ' + object_info['object_bucket'] + '/' + object_info['object_key'])

        # s3_response = s3_client.get_object(Bucket=object_info['object_bucket'], Key=object_info['object_key'], Range="bytes=0-0")
        # storage_class = s3_response.get("StorageClass")
        storage_class = object_summary.storage_class
        if (storage_class is None):
            storage_class = 'STANDARD'

        e_tag = object_summary.e_tag
        if (e_tag is None):
            e_tag = ''
        else:
            e_tag = e_tag.replace('"', '')

        # print('GETTING OBJECT ENCRYPTION')
        sse_encryption = object_summary.Object().server_side_encryption
        if (sse_encryption is None):
            sse_encryption = 'NO'
        if (sse_encryption == 'aws:kms'):
            sse_encryption = 'KMS'

        # etag differences https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html

        # get object tags
        # print('GETTING OBJECT TAGS')
        s3_client = boto3.client(
            "s3", region_name=object_info['bucket_region'])
        s3_response = s3_client.get_object_tagging(
            Bucket=object_info['object_bucket'], Key=object_info['object_key'])
        metadata = s3_response.get("Tags")
        # print('METADATA')
        # print(metadata)
        tags = []
        tag_vals = []
        for tag in s3_response['TagSet']:
            tags.append({tag['Key']: tag['Value']})
            tag_vals.append(tag['Value'])

        # get object name
        items = object_info['object_key'].split('/')
        object_name = items.pop()

        # print(object_name)
        # print(str(os_key))

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

        # json_string = json.dumps(query)
        # byte_ = json_string.encode("utf-8")
        # print('OBJECT SIZE')
        # print(len(byte_))

        # print('OBJECT QUERY')
        # print(query)

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
