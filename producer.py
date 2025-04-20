import copy
import json
import random
import boto3
import time
import logging
import sys
from uuid import uuid4
from collections import namedtuple

logging.basicConfig(
    format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt = '%Y-%m-%d %H:%M:%S',
    level = logging.INFO,
    handlers = [
        logging.FileHandler("producer.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

Product = namedtuple('Product', ['code', 'name', 'price'])

products = [
    Product('A0001', 'Hair Comb', 2.99),
    Product('A0002', 'Toothbrush', 5.99),
    Product('A0003', 'Dental Floss', 0.99),
    Product('A0004', 'Hand Soap', 1.99)
]

seller_ids = ['abc', 'xyz', 'jkq', 'wrp']
customer_ids = [
    "C1000", "C1001", "C1002", "C1003", "C1004", "C1005", "C1006", "C1007", "C1008", "C1009",
    "C1010", "C1011", "C1012", "C1013", "C1014", "C1015", "C1016", "C1017", "C1018", "C1019",
]

def make_order():
    order_id = str(uuid4())
    seller_id = random.choice(seller_ids)
    # print(seller_id)
    customer_id = random.choice(customer_ids)
    order_items = []
    available_products = copy.copy(products)
    n_products = random.randint(1, len(products))

    for _ in range(n_products):
        product = random.choice(available_products)
        available_products.remove(product)

        qty = random.randint(1, 10)

        order_items.append({
            'product_name': product.name,
            'product_code': product.code,
            'product_quantity': qty,
            'product_price': product.price
        })

    order = {
        'customer_id': customer_id,
        'seller_id': seller_id,
        'order_id': order_id,
        'order_items': order_items
    }
    return order

'''
    This function is used to inser batch records
    in the kinesis data streams.
'''
def batchInsert(streamName):
    print('Starting PutRecord Producer')
    kinesis = boto3.client('kinesis')

    order_records = []

    while True:
        order = make_order()

        kinesis_entry = {
            'Data': json.dumps(order).encode('utf-8'),
            'PartitionKey': order['order_id']
        }

        order_records.append((order, kinesis_entry))
        if len(order_records) == 5:
            try:
                orders, records = map(list, zip(*order_records))
                response = kinesis.put_records(Records = records, StreamName = streamName)

                for i, record_response in enumerate(response['Records']):
                    error_code = record_response.get('ErrorCode')
                    o = orders[i]
                    
                    if error_code:
                        err_msg = record_response['ErrorMessage']
                        logging.error(f"Failed to produce record {i} because {err_msg}")
                    else:
                        seq = record_response['SequenceNumber']
                        shard = record_response['ShardId']

                        logging.info(f"Record {i} produced at sequence {seq} to Shard {shard}")
                
                order_records.clear()
            except Exception as e:
                logging.error({
                    'message': 'Error Producing Records',
                    'error': str(e),
                    'records': order_records
                })
            time.sleep(0.3)


'''
    This function is used to insert the records in each
    shards accounding to the sequence no of last records
    to maintain the sequence.
'''
def overrideSequenceNumberInsert(streamName):
    print('Starting PutRecord Producer')
    stream_name = streamName
    kinesis = boto3.client('kinesis')

    partition_sequences = {}
    
    while True:
        order = make_order()
        partition_key = order['seller_id']
        kwargs = dict(StreamName = stream_name, Data = json.dumps(order).encode('utf-8'), PartitionKey=partition_key)

        if partition_key in partition_sequences:
            seq_num = partition_sequences.get(partition_key)
            kwargs.update(SequenceNumberForOrdering = seq_num)

        try:
            response = kinesis.put_record(**kwargs)
            partition_sequences[partition_key] = response['SequenceNumber']
            logging.info(f"Produced Record {response['SequenceNumber']} to Shard {response['ShardId']}")
        except Exception as e:
            logging.error({
                'message' : 'Error Producing Record',
                'error': str(e),
                'record': order
            })
        time.sleep(0.3)

'''
    This function is used to insert one record at a time in
    kinesis data streams.
'''
def insertRecord(streamName, totalRecords):
    logging.info("Starting PutRecord Producer")
    stream_name = streamName
    kinesis = boto3.client('kinesis')

    for i in range(totalRecords):
        order = make_order()
        try:
            response = kinesis.put_record(
                StreamName = stream_name,
                Data = json.dumps(order).encode('utf-8'),
                PartitionKey = order['order_id']
            )
            logging.info(f"Produced Record {i} {response['SequenceNumber']} to Shard {response['ShardId']}")
        except Exception as e:
            logging.error(e)
        time.sleep(1)
        i += 1


def lambda_handler(event, context):
    # insertRecord('kinesis-demo', 1000)
    # overrideSequenceNumberInsert('kinesis-demo')
    batchInsert('kinesis-demo')

lambda_handler(1, 1)
