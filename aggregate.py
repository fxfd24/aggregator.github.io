from datetime import datetime
import pandas as pd
import bson

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
        period_range = pd.period_range(start=dt_from, end=dt_upto, freq='H')
    elif group_type == 'day':
        grouped_data = df.groupby(pd.Grouper(key='timestamp', freq='D')).sum()
        period_range = pd.period_range(start=dt_from, end=dt_upto, freq='D')
    elif group_type == 'month':
        grouped_data = df.groupby(pd.Grouper(key='timestamp', freq='M')).sum()
        period_range = pd.period_range(start=dt_from, end=dt_upto, freq='M')
    
    # print('grouped_data-',grouped_data)
    full_range = pd.date_range(start=dt_from, end=dt_upto, freq=group_type[0].upper())
    aggregated = grouped_data.reindex(full_range, fill_value=0)['value'].tolist()

    # Преобразование каждого периода в начало месяца и преобразование в строку
    labels = [period.start_time.strftime('%Y-%m-%dT%H:%M:%S') for period in period_range]
    
    return {"dataset": aggregated, "labels": labels}



# Пример использования
input_data = {
    "dt_from": "2022-02-01T00:00:00",
    "dt_upto": "2022-02-02T00:00:00",
    "group_type": "hour"
}

file_path = 'sample_collection.bson'
data = load_data_from_bson(file_path)
output = aggregate_data(input_data['dt_from'], input_data['dt_upto'], input_data['group_type'], data)
print(output)

