import json
import logging
import sys
import time
import boto3

logging.basicConfig(
    format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt = '%Y-%m-%d %H:%M:%S',
    level = logging.INFO,
    handlers = [
        logging.FileHandler("consumer.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

class ShardIteratorPair:
    def __init__(self, shard_id, iterator):
        self.shard_id = shard_id
        self.iterator = iterator

def getRecords(streamName):
    logging.info("Starting GetRecords Consumer")
    stream_name = streamName
    kinesis = boto3.client('kinesis')
    
    logging.info('Fetching Shards and Iterators')
    shard_iterators = []

    shard_response = kinesis.list_shards(StreamName = stream_name)
    has_more = bool(shard_response['Shards'])

    while has_more:
        for shard in shard_response['Shards']:
            shard_id = shard['ShardId']
            itr_response = kinesis.get_shard_iterator(StreamName = stream_name, ShardId = shard_id, ShardIteratorType = 'TRIM_HORIZON')
            shard_itr = ShardIteratorPair(shard_id=shard_id, iterator=itr_response)
            shard_iterators.append(shard_itr)

        if 'NextToken' in shard_response:
            # kinesis does not lists all the shards in one go if there was many shards then use this syntax
            shard_response = kinesis.list_shards(StreamName = stream_name, NextToken=shard_response['NextToken'])
            has_more = bool(shard_response['Shards'])
        else:
            has_more = False

    while True:
        for shard_itr in shard_iterators:
            try:
                records_response = kinesis.get_records(ShardIterator=shard_itr.iterator['ShardIterator'], Limit=200)

                for record in records_response['Records']:
                    order = json.loads(record['Data'].decode('utf-8'))
                    logging.info(f"Read Order {order} from Shard {shard_itr.shard_id} at position {record['SequenceNumber']}")
                
                if records_response['NextShardIterator']:
                    shard_itr.iterator['ShardIterator'] = records_response['NextShardIterator']

            except Exception as e:
                if str(e) == "\'NextShardIterator\'":
                    logging.info("Waiting for new streams....")
                else:
                    logging.error({
                        'message': 'Failed fetching records',
                        'error': str(e)
                    })
        
        time.sleep(0.5)
