import logging
import boto3
import traceback
import os
import requests
import json
from datetime import datetime

bucket_name = os.environ['BUCKET_NAME']
slack_url = os.environ['SLACK_URL']
log_level = os.environ.get('LOG_LEVEL', 'ERROR')
slack_channel = os.environ.get('SLACK_CHANNEL', '#general')


def send_message(content, channel):
    payload_dic = {
        "text": content,
        "channel": channel,
    }
    requests.post(slack_url, data=json.dumps(payload_dic))


def get_value(e, column):
    return str(e.get(column, '')).replace(',', '')

def logger_level(level):
    if level == 'CRITICAL':
        return 50
    elif level == 'ERROR':
        return 40
    elif level == 'WARNING':
        return 30
    elif level == 'INFO':
        return 20
    elif level == 'DEBUG':
        return 10
    else:
        return 0

logger = logging.getLogger()
logger.setLevel(logger_level(log_level))


def lambda_handler(event, context):
    try:
        s3_bucket = boto3.resource('s3').Bucket(bucket_name)

        body = ''
        for e in event:
            rows = []
            rows.append(get_value(e, 'email'))
            rows.append(get_value(e, 'timestamp'))
            rows.append(get_value(e, 'ip'))
            rows.append(get_value(e, 'sg_event_id'))
            rows.append(get_value(e, 'sg_message_id'))
            rows.append(get_value(e, 'useragent'))
            rows.append(get_value(e, 'event'))
            rows.append(get_value(e, 'response'))
            rows.append(get_value(e, 'tls'))

            body += ','.join(rows)
            body += '\n'


        key_name = '{time}{id}.csv'.format(
            time=datetime.now().strftime('%Y/%m/%d/%H/%M%S'),
            id=event[0].get('sg_event_id', '')
        )

        s3_bucket.put_object(
            Key=key_name,
            Body=body,
            ContentType='text/csv'
        )

    except:
        logger.error(traceback.format_exc())
        send_message(traceback.format_exc(), slack_channel)
