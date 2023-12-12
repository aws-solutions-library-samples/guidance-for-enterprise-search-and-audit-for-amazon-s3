import json
import boto3
import requests
import base64
import csv
import urllib.parse
import os
import datetime
from aws_requests_auth.aws_auth import AWSRequestsAuth

ssm_client = boto3.client('ssm')

host = ssm_client.get_parameter(
    Name="s3auditor-opensearch-domain")['Parameter']['Value']
region = ssm_client.get_parameter(
    Name="s3auditor-opensearch-region")['Parameter']['Value']
export_bucket = ssm_client.get_parameter(
    Name="s3auditor-export-bucket")['Parameter']['Value']
service = 'opensearchservice'

credentials = boto3.Session().get_credentials()
auth = AWSRequestsAuth(aws_access_key=os.environ['AWS_ACCESS_KEY_ID'],
                       aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                       aws_token=os.environ['AWS_SESSION_TOKEN'],
                       aws_host=host,
                       aws_region=region,
                       aws_service='es')

headers = {"Content-Type": "application/json"}
os_fields = ["_index", "aws_account", "prefix", "tags", "bucket", "object_name", "size",
             "last_read", "last_write", "region", "storage_class", "sse_encryption", "etag", "deleted"]


def lambda_handler(event, context):

    query_size = 100

    query = {
        "size": 100,
        "sort": [{'object_name': 'asc'}],
        "query": {},
        "fields": os_fields
    }

    if (event['t'] == 'path'):
        bucket = event['bucket'] if len(event['bucket']) > 0 else ''

        query['query'] = {"bool": {"must": [{"match": {"bucket": bucket}}]}}

        if (len(event['path']) > 0):
            query['query']['bool']['must'].append(
                {"term": {"prefix": event['path']}})

        filter_objects = parse_filters(event['filters'])
        for i in filter_objects:
            query['query']['bool']['must'].append(i)

        search_results = run_objects_query(query)

    elif (event['t'] == 'search'):

        # if query contains a : then assume it's a tag search, otherwise it's a normal search
        if (':' in event['q']):

            tag_search = event['q'].split(':')
            tag_name = 'tags.' + tag_search[0]
            query['query'] = {"nested": {"path": "tags", "query": {
                "bool": {"must": [{"match": {tag_name: tag_search[1]}}]}}}}

        else:
            query['query'] = {"match": {"search_field": event['q']}}

        search_results = run_objects_query(query)

    search_after = ''

    today = datetime.datetime.now()
    inventory_filename = 'inventory_export_' + \
        today.strftime("%Y%m%d%H%M%S%f") + '.csv'
    inventory_csv = csv.writer(open("/tmp/" + inventory_filename, "w+"))
    inventory_csv.writerow(["Account", "Region", "Bucket", "Path", "Object", "Size", "Latest Write",
                           "Latest Read", "Storage Class", "Encryption", "Etag", "Deleted", "Tags"])

    while ('hits' in search_results and 'hits' in search_results['hits'] and len(search_results['hits']['hits']) > 0):
        for hit in search_results['hits']['hits']:
            result_row = hit['_source']

            tags_string = ''
            if ('tags' in result_row):
                for tag in result_row['tags']:
                    for key, value in tag.items():
                        tags_string += key + ':' + value + '|'

            csv_row = [
                result_row['aws_account'] if 'aws_account' in result_row else '',
                result_row['region'] if 'region' in result_row else '',
                result_row['bucket'] if 'bucket' in result_row else '',
                result_row['prefix'] if 'prefix' in result_row else '',
                result_row['object_name'] if 'object_name' in result_row else '',
                result_row['size'] if 'size' in result_row else '',
                result_row['last_write'] if 'last_write' in result_row else '',
                result_row['last_read'] if 'last_read' in result_row else '',
                result_row['storage_class'] if 'storage_class' in result_row else '',
                result_row['sse_encryption'] if 'sse_encryption' in result_row else '',
                result_row['etag'] if 'etag' in result_row else '',
                'Yes' if 'deleted' in result_row and result_row['deleted'] == True else 'No',
                tags_string if 'tags' in result_row else ''
            ]

            inventory_csv.writerow(csv_row)

            search_after = hit['sort']

        query['search_after'] = search_after
        search_results = run_objects_query(query)

    client = boto3.client('s3')
    client.upload_file("/tmp/" + inventory_filename,
                       export_bucket, inventory_filename)

    # print('Done')
    return


def run_objects_query(query):
    url = 'https://' + host + '/s3-objects/_search'
    result = requests.get(url, headers=headers,
                          data=json.dumps(query), auth=auth, timeout=20)
    obj = json.loads(result.text)

    # print('OS Query')
    # print(obj)

    return obj


def parse_filters(filters):
    filter_objects = []
    if ('object_name' in filters and len(filters['object_name'])):
        filter_objects.append(
            {"wildcard": {"object_name": "*" + filters['object_name'] + "*"}})

    if ('size_from' in filters and len(filters['size_from'])):
        in_bytes = float(filters['size_from']) * 1024
        filter_objects.append({"range": {"size": {"gte": in_bytes}}})

    if ('size_to' in filters and len(filters['size_to'])):
        in_bytes = float(filters['size_to']) * 1024
        filter_objects.append({"range": {"size": {"lte": in_bytes}}})

    if ('write' in filters and len(filters['write'])):
        two_points = filters['write'].split('-')
        # format two dates for range query
        date_from = datetime.datetime.strptime(
            two_points[0].strip(), "%d/%m/%y").date()
        date_to = datetime.datetime.strptime(
            two_points[1].strip(), "%d/%m/%y").date()

        compare_string_date_from = date_from.strftime('%Y-%m-%d') + ' 00:00:00'
        compare_string_date_to = date_to.strftime('%Y-%m-%d') + ' 23:23:59'

        filter_objects.append({"range": {"last_write": {
                              "gte": compare_string_date_from, "lte": compare_string_date_to}}})

    if ('read' in filters and len(filters['read'])):
        two_points = filters['read'].split('-')
        # format two dates for range query
        date_from = datetime.datetime.strptime(
            two_points[0].strip(), "%d/%m/%y").date()
        date_to = datetime.datetime.strptime(
            two_points[1].strip(), "%d/%m/%y").date()

        compare_string_date_from = date_from.strftime('%Y-%m-%d') + ' 00:00:00'
        compare_string_date_to = date_to.strftime('%Y-%m-%d') + ' 23:23:59'

        filter_objects.append({"range": {"last_read": {
                              "gte": compare_string_date_from, "lte": compare_string_date_to}}})

    return filter_objects
