import boto3
import string
import json
import os
import csv
import datetime
import logging
import sys

region = sys.argv[0]  # AWS REGION WHERE S3 AUDITOR IS HOSTED
account = sys.argv[1]  # AWS ACCOUNT NUMBER WHERE S3 AUDITOR IS HOSTED
inventory_filename = sys.argv[2]  # full_inventory.csv
queue_name = sys.argv[3]  # s3auditor-object-activity
log_filename = sys.argv[4]  # inventory_process.log

logging.basicConfig(filename=log_filename, filemode='w', level=logging.INFO)

sqs = boto3.resource('sqs', region_name=region)
queue = sqs.get_queue_by_name(QueueName=queue_name)

cnt = 1
with open(inventory_filename) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    batch_count = 1
    batch = []
    for row in csv_reader:
        bucket = row[0]
        object_name = row[1]
        size = row[2]
        modified = row[3]

        # remove .000Z from the modified time in the export
        modified = datetime.datetime.strptime(
            modified, '%Y-%m-%dT%H:%M:%S.000Z').strftime("%Y-%m-%dT%H:%M:%SZ")

        inventory_item = {
            "version": "0",
            "id": "9488ca24-c116-b5b8-204f-d34d6acc863a",
            "detail-type": "Object Created",
            "source": "aws.s3",
            "account": account,
            "time": modified,
            "region": region,
            "resources": [
                "arn:aws:s3:::" + bucket
            ],
            "detail": {
                "version": "0",
                "bucket": {
                    "name": bucket
                },
                "object": {
                    "key": object_name,
                    "size": size,
                    "etag": "",
                    "sequencer": ""
                },
                "request-id": "",
                "requester": "",
                "source-ip-address": "",
                "reason": "PutObject"
            }
        }

        inventory_queue_item = {
            "Type": "Notification",
            "MessageId": "a679a2ef-b46d-5231-8784-fe3bf1708c93",
            "Timestamp": modified,
            "Message": json.dumps(inventory_item)
        }

        batch.append(
            {"Id": str(batch_count), "MessageBody": json.dumps(inventory_queue_item)})

        logging.info('object added: ' + str(cnt) + ' - ' + object_name)

        if batch_count == 10:
            response = queue.send_messages(Entries=batch)

            if 'Failed' in response:
                for msg_meta in response['Failed']:
                    logging.info(
                        "Failed to send: %s: %s",
                        msg_meta['MessageId'],
                    )
                logging.info(json.dumps(batch))

            batch = []
            batch_count = 0

        batch_count += 1
        cnt += 1

    if len(batch):
        response = queue.send_messages(Entries=batch)
