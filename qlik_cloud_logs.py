# import libraries
from dotenv import load_dotenv
from qlik_sdk import Auth, AuthType, Config
import os
from datetime import datetime, timedelta
import requests
import boto3

# load variables from .env file in same directory
load_dotenv()

# get env variables
QLIK_HOST = os.getenv('QLIK_HOST')
QLIK_KEY = os.getenv('QLIK_KEY')
DYNATRACE_KEY = os.getenv('DYNATRACE_KEY')
DYNATRACE_HOST = os.getenv('DYNATRACE_HOST')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')
AWS_BUCKET_KEY = os.getenv('AWS_BUCKET_KEY')

# setup constant variables
MINUTES = int(os.getenv('MINUTES_TO_GET'))
EVENT_FILENAME = 'last_event_call.txt'
RELOAD_FILENAME = 'last_reload_call.txt'

# connect to s3
session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)
s3 = session.client('s3')

# connect to Qlik SDK
client = Auth(
    config=Config(
        auth_type=AuthType.APIKey,
        host=f'https://{QLIK_HOST}',
        api_key=QLIK_KEY,
    )
)

def get_s3_file(file_name):
    try:
        obj = s3.get_object(Bucket=AWS_BUCKET_NAME, Key=file_name)
        return obj['Body'].read().decode('utf-8').strip()
    except:
        return None

def get_times():
    now = datetime.utcnow()
    time_end = now.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    # check if file exists and if it does then use the start time from the file which should be the last time the call was made
    time_start = get_s3_file(EVENT_FILENAME)
    if time_start == None:
        time_diff = timedelta(minutes=MINUTES)
        time_start = (now - time_diff).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    last_reload = get_s3_file(RELOAD_FILENAME)
    if last_reload == None:
        time_diff = timedelta(minutes=MINUTES)
        last_reload = (now - time_diff).strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-3] + 'Z'
    return {'last_reload': last_reload, 'time_start': time_start, 'time_end': time_end}    

# function which posts data to dynatrace
def add_logs(data, typ):
    headers = {
        'accept': 'application/json; charset=utf-8',
        'Content-Type': 'application/json; charset=utf-8',
        "Authorization": f"Api-Token {DYNATRACE_KEY}"
    }
    requests.post(
        f'https://{DYNATRACE_HOST}/api/v2/logs/ingest', headers=headers, json=data)

# function which determines severity of audit/reload log
def get_severity(data, event=True):
    if event:
        return "info"
    if data["status"] == "FAILED" or data["status"] == "EXCEEDED_LIMIT":
        return "error"
    return "info"

def get_space_name(space_id):
    response = client.rest(path=f'/spaces/{space_id}').json()
    return response['name']

def get_space_id_and_name_from_app(app_id):
    app = client.rest(path=f'/apps/{app_id}').json()
    space_id = app['attributes'].get('spaceId', None)
    if space_id is not None:
        return {
            'space_name': get_space_name(space_id),
            'space_id': space_id
        }
    return {
        'space_name': 'Personal',
        'space_id': 'Personal'
    }

# function which gets qlik audit events and posts them to dynatrace
def get_events_and_add_logs(client, path, time_end):
    while path:
        response = client.rest(path=path)
        data = response.json()
        # transforming data to match dynatrace desired format
        transformed_data = []
        for d in data['data']:
            if 'spaceId' in d['data']:
                d['spaceId'] = d['data']['spaceId']
                d['spaceName'] = get_space_name(d['data']['spaceId'])
                transformed_data.append({
                    'Content': d,
                    'Timestamp': d['eventTime'],
                    'log.source': 'qlik-cloud',
                    'severity': get_severity(d)
                })
        if len(transformed_data) > 0:        
            add_logs(transformed_data, 'events')
        # paging for more events
        next_link = data['links']['Next']['Href'] if data['links']['Next'] is not None else None
        path = None if next_link == None else next_link.split('/api/v1')[1]
    write_s3_file(EVENT_FILENAME, str(time_end))
    return

def validate_end_time(end_time):
    if isinstance(end_time, str):
        try:
            return datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%S.%fZ')
        except ValueError:
            raise ValueError(
                'Invalid datetime format. Expected format: YYYY-MM-DDTHH:MM:SS.sssZ')
    elif not isinstance(end_time, datetime):
        raise TypeError(
            'Invalid type for end_time. Expected str or datetime.datetime')
    return end_time

def transform_data(d):
    if d['status'] != "QUEUED" and d['status'] != "":
        space_id_and_name = get_space_id_and_name_from_app(d['appId'])
        d['spaceId'] = space_id_and_name['space_id']
        d['spaceName'] = space_id_and_name['space_name']
        return {
            'Content': d,
            'Timestamp': d['endTime'],
            'log.source': 'qlik-cloud',
            'severity': get_severity(d)
        }
    return None

def get_next_url(response):
    if 'links' in response and 'next' in response['links'] and 'href' in response['links']['next']:
        return response['links']['next']['href'].split('/api/v1')[1]
    return None

def get_current_end_time(item):
    end_time_str = item.get('endTime')
    status = item.get('status')
    if end_time_str and status != 'QUEUED' and status != 'CANCELING':
        return datetime.strptime(end_time_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    return None

def write_s3_file(file, content):
    s3.put_object(Bucket=AWS_BUCKET_NAME, Key=file, Body=content)

def get_reloads_and_add_logs(end_time):
    end_time = validate_end_time(end_time)
    latest_time = None
    response = client.rest(path='/reloads').json()
    loop = True
    while loop == True:
        if len(response['data']) == 0:
            loop = False
        for d in response['data']:
            current_end_time = get_current_end_time(d)
            if current_end_time is not None:
                if current_end_time > end_time:
                    transformed_data = transform_data(d)
                    if transformed_data is not None:
                        add_logs([transformed_data], 'reloads')
                        # Update latest_time if current_end_time is greater
                        if latest_time is None or current_end_time > latest_time:
                            latest_time = current_end_time
                else:
                    loop = False
        next_url = get_next_url(response)
        if next_url and (latest_time is None or latest_time > end_time):
            response = client.rest(path=next_url).json()
        else:
            loop = False

    # Only write if latest_time has been updated
    if latest_time is not None:
        write_s3_file(RELOAD_FILENAME, latest_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z')

if __name__ == '__main__':
    times = get_times()
    time_start, time_end, last_reload = times['time_start'], times['time_end'], times['last_reload']
    get_events_and_add_logs(client, path=f'/audits?eventTime={time_start}/{time_end}', time_end=time_end)
    get_reloads_and_add_logs(last_reload)
