import json
import boto3

def lambda_handler(event, context):
    
    ssm_client = boto3.client('ssm')
    queue_url = ssm_client.get_parameter(Name="s3auditor-export-queue-url")['Parameter']['Value']
    export_bucket = ssm_client.get_parameter(Name="s3auditor-export-bucket")['Parameter']['Value']
    
    sqs_client = boto3.client("sqs")

    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(event['query'])
    )
    
    return {
        'statusCode': 200,
        'body': 'Done'
    }
