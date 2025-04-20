import base64
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def decode_record(record: bytes) -> dict:
    string_data = base64.b64decode(record).decode('utf-8')
    return json.loads(string_data)

def lambda_handler(event, context):
    logger.info(f"Orders Transform Handler Invoked with Records {event['records'][:2]}")
    output = []

    for record in event['records']:

        payload = decode_record(record['data'])
        order_items = payload.pop('order_items') # remove order_items from data
        total = sum([oi['product_quantity'] * oi['product_price'] for oi in order_items])
        payload['order_total'] = total # add new columns in data
        payload['order_items'] = order_items # add order_items from data

        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(json.dumps(payload).encode('utf-8'))
        }
        output.append(output_record)
    
    return {'records': output}
