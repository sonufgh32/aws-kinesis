import copy
import json
import random
import boto3
import time
import logging
from decimal import Decimal
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

def createDynamoDBTable(tableName):
    dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')

    if tableName in dynamodb.meta.client.list_tables()['TableNames']:
        logging.info(f"DynamoDB Table {tableName} already exists!!!")
        return
    
    try:
        response = dynamodb.create_table(
            TableName=tableName,
            KeySchema=[
                {
                    "AttributeName": "order_id",
                    "KeyType": "HASH" # Partition Key
                },
                {
                    "AttributeName": "seller_id",
                    "KeyType": "RANGE" # Sort Key
                }
            ],
            AttributeDefinitions=[ # AttributeDefinitions container only columns that are part of KeySchema, GSI, LSI
                {
                    "AttributeName": "customer_id",
                    "AttributeType": "S",
                },
                {
                    "AttributeName": "seller_id",
                    "AttributeType": "S",
                },
                {
                    "AttributeName": "order_id",
                    "AttributeType": "S",
                },
                # {
                #     "AttributeName": "order_items",
                #     "AttributeType": "S"
                # }
            ],
            LocalSecondaryIndexes=[
                {
                    "IndexName": "OrderIndex",
                    "KeySchema": [
                        {
                            "AttributeName": "order_id",
                            "KeyType": "HASH" # Partition Key
                        },
                        {
                            "AttributeName": "customer_id",
                            "KeyType": "RANGE" # Sort Key
                        }
                    ],
                    "Projection": {
                        "ProjectionType": "ALL" # ALL, KEYS_ONLY(Partition & Sort Key), INCLUDE
                        # "NonKeyAttributes": [""] # Only for INCLUDE type, provide list of columns names
                    }
                }
            ],
            # GlobalSecondaryIndexed=[
            #     {
            #         "IndexName": "OrderIndex",
            #         "KeySchema": [
            #             {
            #                 "AttributeName": "seller_id",
            #                 "KeyType": "HASH"
            #             }
            #         ],
            #         "Projection": {
            #             "ProjectionType": "ALL" # ALL, KEYS_ONLY(Partition & Sort Key), INCLUDE
            #             # "NonKeyAttributes": [""] # Only for INCLUDE type, provide list of columns names
            #         },
            #         "ProvisionedThroughput": {
            #             "ReadCapacityUnits": 5,
            #             "WriteCapacityUnits": 5
            #         }
            #     }
            # ],
            ProvisionedThroughput={
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 5
            }
        )

        # logging.info(f"Response: {response}")

        dynamodb.meta.client.get_waiter('table_exists').wait(TableName=tableName) # table_not_exists
        
        logging.info(f"DynamoDB Table {tableName} created successfully!!!")

    except Exception as e:
        logging.error({
            'message': 'Error Creating Table',
            'error': str(e)
        })

def insertRecord(tableName, recordCounts):
    dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')
    tbl = dynamodb.Table(tableName)

    for i in range(recordCounts):
        response = tbl.put_item(Item=json.loads(json.dumps(make_order()), parse_float=Decimal))
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logging.info(f"Record {i} inserted successfully!")
        else:
            logging.info("Something went wrong!!!")


if __name__ == '__main__':
    # createDynamoDBTable('orders')
    insertRecord('orders', 1000)
