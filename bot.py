import json
import asyncio
import requests
from datetime import datetime
import pandas as pd
import bson

TOKEN = '6579753297:AAEcJNNZAC_tl1PyvxO9hyNCcWcY57JPBVg'
API_URL = f'https://api.telegram.org/bot{TOKEN}/'

def load_data_from_bson(file_path):
    with open(file_path, 'rb') as f:
        data = bson.decode_all(f.read())
    return data

def aggregate_data(dt_from, dt_upto, group_type, data):
    dt_from = datetime.fromisoformat(dt_from)
    dt_upto = datetime.fromisoformat(dt_upto)
    
    filtered_data = [
        {'timestamp': pd.to_datetime(d['dt'], errors='coerce'), 'value': d['value']}
        for d in data
        if isinstance(d['dt'], str) or isinstance(d['dt'], datetime)
    ]
    
    filtered_data = [
        d for d in filtered_data
        if d['timestamp'] is not pd.NaT and dt_from <= d['timestamp'] <= dt_upto
    ]
    
    df = pd.DataFrame(filtered_data)
    
    if group_type == 'hour':
        grouped_data = df.groupby(pd.Grouper(key='timestamp', freq='H')).sum()
    elif group_type == 'day':
        grouped_data = df.groupby(pd.Grouper(key='timestamp', freq='D')).sum()
    elif group_type == 'month':
        grouped_data = df.groupby(pd.Grouper(key='timestamp', freq='M')).sum()
    
    full_range = pd.date_range(start=dt_from, end=dt_upto, freq=group_type[0].upper())
    
    aggregated = grouped_data.reindex(full_range, fill_value=0)['value'].tolist()
    labels = [label.strftime('%Y-%m-%dT%H:%M:%S') for label in full_range]
    
    return {"dataset": aggregated, "labels": labels}

async def start_message(message):
    chat_id = message['chat']['id']
    await send_message(chat_id, "Привет! Я бот для агрегации данных. Пожалуйста, отправьте JSON с входными данными.")

async def handle_message(message):
    chat_id = message['chat']['id']
    json_data = json.loads(message['text'])
    aggregated_data = aggregate_data(json_data['dt_from'], json_data['dt_upto'], json_data['group_type'], data)
    await send_message(chat_id, json.dumps(aggregated_data))

async def send_message(chat_id, text):
    url = f"{API_URL}sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': text
    }
    response = requests.post(url, json=payload)
    return response.json()

async def handle_updates(updates):
    for update in updates:
        if 'message' in update:
            message = update['message']
            if 'text' in message:
                if message['text'].startswith('/start'):
                    await start_message(message)
                else:
                    await handle_message(message)

async def get_updates(offset=None):
    url = f"{API_URL}getUpdates"
    params = {'offset': offset, 'timeout': 60}
    response = requests.get(url, params=params)
    return response.json()['result']

async def main():
    offset = None
    while True:
        updates = await get_updates(offset)
        if updates:
            await handle_updates(updates)
            offset = updates[-1]['update_id'] + 1
        await asyncio.sleep(1)

if __name__ == "__main__":
    file_path = 'sample_collection.bson'
    data = load_data_from_bson(file_path)
    asyncio.run(main())
