import json
import boto3
import requests
import base64
import urllib.parse
import os
import datetime
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

    query_size = int(qs['pp'])
    page = int(qs['p'])
    query_from = (int(page) - 1) * query_size

    db = {
        'prefixInfo': {},
        'items': []
    }

    sort_col = qs['s']
    sort_dir = qs['sd']

    if ('q' not in qs):
        qs['q'] = ''

    if (qs['t'] == 'path'):
        # a if condition else b
        bucket = qs['b'] if len(qs['b']) > 0 else ''

        query = {
            # "from":query_from,"size":query_size,
            "size": query_size,
            "sort": [{''.join(sort_col): sort_dir}],
            "query": {"bool": {"must": [{"match": {"bucket": bucket}}, {"match": {"prefix": qs['q']}}]}},
            "fields": ["_index", "aws_account", "prefix", "tags", "bucket", "object_name", "size", "last_read", "last_write", "region", "deleted"],
            "_source": "false"
        }

        filter_objects = parse_filters(qs)
        for i in filter_objects:
            query['query']['bool']['must'].append(i)

        search_results = run_objects_query(query)
        region = search_results['hits']['hits'][0]['fields']['region'][0]

        # return query
        print(search_results)

        # get total size of the prefix
        query = {
            "size": 0,
            "query": {"bool": {"must": [{"term": {"bucket": bucket}}]}},
            "aggs": {"total_size": {"sum": {"field": "size"}}, "total_count": {"value_count": {"field": "size"}}, "avg_size": {"avg": {"field": "size"}}}
        }

        if (len(qs['q']) > 0):
            query['query']['bool']['must'].append(
                {"term": {"prefix": qs['q']}})

        for i in filter_objects:
            query['query']['bool']['must'].append(i)

        results = run_objects_query(query)
        total_size = results['aggregations']['total_size']['value']
        total_count = results['aggregations']['total_count']['value']
        avg_size = results['aggregations']['avg_size']['value']

        # get prefix info
        path = qs['q'].split('/')
        prefix = path.pop()
        parent = "/".join(str(x) for x in path)

        query = {
            "size": 1,
            "query": {"bool": {"must": [{"term": {"bucket": bucket}}, {"term": {"parent": parent}}, {"term": {"prefix": prefix}}]}}
        }
        results = run_prefix_query(query)

        prefix_info = results['hits']['hits'][0]['_source']

        db['prefixInfo'] = {
            'account': prefix_info['aws_account'],
            'bucket': prefix_info['bucket'],
            'prefix': parent + '/' + prefix,
            'total_size': total_size,
            'avg_size': avg_size if avg_size is not None else 0,
            'total_count': total_count,
            'last_write': prefix_info['last_write'],
            'region': region,
            'last_read': prefix_info['last_read'] if 'last_read' in prefix_info else '',
        }

    elif (qs['t'] == 'search'):

        # if query contains a : then assume it's a tag search, otherwise it's a normal search
        if (':' in qs['q']):
            tag_search = qs['q'].split(':')
            tag_name = 'tags.' + tag_search[0]
            query = {
                "from": query_from, "size": query_size,
                "sort": [{''.join(sort_col): sort_dir}],
                "query": {"nested": {"path": "tags", "query": {"bool": {"must": [{"match": {tag_name: tag_search[1]}}]}}}},
                "fields": ["_index", "aws_account", "prefix", "tags", "bucket", "object_name", "size", "last_read", "last_write", "region", "deleted"],
                "aggs": {"total_size": {"sum": {"field": "size"}}, "total_count": {"value_count": {"field": "size"}}, "avg_size": {"avg": {"field": "size"}}},
                "_source": "false"
            }

        else:
            query = {
                "from": query_from, "size": query_size,
                # "sort": [{''.join(sort_col):sort_dir}],
                "sort": [{"_score": {"order": "desc"}}, {''.join(sort_col): sort_dir}],
                # "query": {"match": {"search_field":qs['q']}},
                "query": {"multi_match": {"query": qs['q'], "fields": ["object_name^10", "bucket^7", "aws_account^5", "region^2", "search_field"]}},
                "fields": ["_index", "aws_account", "prefix", "tags", "bucket", "object_name", "size", "last_read", "last_write", "region", "deleted"],
                "aggs": {"total_size": {"sum": {"field": "size"}}, "total_count": {"value_count": {"field": "size"}}, "avg_size": {"avg": {"field": "size"}}},
                "_source": "false"
            }

        search_results = run_objects_query(query)
        total_size = search_results['aggregations']['total_size']['value']
        total_count = search_results['aggregations']['total_count']['value']
        avg_size = search_results['aggregations']['avg_size']['value']

        db['prefixInfo'] = {
            'total_size': total_size,
            'avg_size': avg_size if avg_size is not None else 0,
            'total_count': total_count
        }

    if ('hits' in search_results and 'hits' in search_results['hits']):
        for hit in search_results['hits']['hits']:
            child = {
                'index': hit['_index'],
                'bucket': hit['fields']['bucket'][0],
                'region': hit['fields']['region'][0] if 'region' in hit['fields'] else '',
                'account': hit['fields']['aws_account'][0],
                'prefix': hit['fields']['prefix'][0],
                'object_name': hit['fields']['object_name'][0],
                'deleted': hit['fields']['deleted'][0] if 'deleted' in hit['fields'] else False,
                'size': hit['fields']['size'][0],
                'tags': hit['fields']['tags'] if 'tags' in hit['fields'] else {},
                'last_read': hit['fields']['last_read'][0] if 'last_read' in hit['fields'] else '',
                'last_write': hit['fields']['last_write'][0],
                'sort_after': hit['sort'][0],
            }

            if ('tags' in hit['fields']):
                child['tags'] = hit['fields']['tags']

            db['items'].append(child)

    return db


def run_objects_query(query):
    url = 'https://' + host + '/s3-objects/_search/'
    result = requests.get(url, headers=headers,
                          data=json.dumps(query), auth=auth, timeout=20)
    obj = json.loads(result.text)

    return obj


def run_prefix_query(query):
    url = 'https://' + host + '/s3-prefixes/_search/'
    result = requests.get(url, headers=headers,
                          data=json.dumps(query), auth=auth, timeout=20)
    obj = json.loads(result.text)

    return obj


def parse_filters(filters):
    filter_objects = []
    if ('f_object_name' in filters and len(filters['f_object_name'])):
        filter_objects.append(
            {"wildcard": {"object_name": "*" + filters['f_object_name'] + "*"}})

    if ('f_size_from' in filters and len(filters['f_size_from'])):
        in_bytes = float(filters['f_size_from']) * 1024
        filter_objects.append({"range": {"size": {"gte": in_bytes}}})

    if ('f_size_to' in filters and len(filters['f_size_to'])):
        in_bytes = float(filters['f_size_to']) * 1024
        filter_objects.append({"range": {"size": {"lte": in_bytes}}})

    if ('f_write' in filters and len(filters['f_write'])):
        two_points = filters['f_write'].split('-')
        # format two dates for range query
        date_from = datetime.datetime.strptime(
            two_points[0].strip(), "%d/%m/%y").date()
        date_to = datetime.datetime.strptime(
            two_points[1].strip(), "%d/%m/%y").date()

        compare_string_date_from = date_from.strftime('%Y-%m-%d') + ' 00:00:00'
        compare_string_date_to = date_to.strftime('%Y-%m-%d') + ' 23:23:59'

        filter_objects.append({"range": {"last_write": {
                              "gte": compare_string_date_from, "lte": compare_string_date_to}}})

    if ('f_read' in filters and len(filters['f_read'])):
        two_points = filters['f_read'].split('-')
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
