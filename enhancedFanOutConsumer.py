import boto3
import json
import time
import sys
import logging

logging.basicConfig(
    format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt = '%Y-%m-%d %H:%M:%S',
    level = logging.INFO,
    handlers = [
        logging.FileHandler("consumer.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

kinesis_client = boto3.client('kinesis')

def getConsumerArn():
    response = kinesis_client.register_stream_consumer(
        StreamARN = '<REPLACE_WITH_KINESIS_DATA_STREAM_ARN>',
        ConsumerName = 'Python-Enchanced-Consumer'
    )
    return response['Consumer']['ConsumerARN']

consumer_arn = getConsumerArn()

time.sleep(5)

stream_description = kinesis_client.describe_stream(StreamName='kinesis-demo')

for shard in stream_description['StreamDescription']['Shards']:
    response = kinesis_client.subscribe_to_shard(
        ConsumerARN = consumer_arn,
        ShardId = shard['ShardId'],
        StartingPosition = {'Type': 'TRIM_HORIZON'}
    )

    for event in response['EventStream']:
        for record in event['SubscribeToShardEvent']['Records']:
            logging.info(f"Record: {record['Data'].decode('utf-8')}")
