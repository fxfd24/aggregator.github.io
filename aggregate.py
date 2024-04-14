from datetime import datetime
import pandas as pd
import bson

def load_data_from_bson(file_path):
    with open(file_path, 'rb') as f:
        data = bson.decode_all(f.read())
    return data

def aggregate_data(dt_from, dt_upto, group_type, data):
    # Преобразование строковых дат в формат datetime
    dt_from = datetime.fromisoformat(dt_from)
    dt_upto = datetime.fromisoformat(dt_upto)
    
    # Фильтрация данных по временному интервалу и преобразование строковых дат в datetime
    filtered_data = [
        {'timestamp': pd.to_datetime(d['dt'], errors='coerce'), 'value': d['value']}
        for d in data
        if isinstance(d['dt'], str) or isinstance(d['dt'], datetime)
    ]
    
    filtered_data = [
        d for d in filtered_data
        if d['timestamp'] is not pd.NaT and dt_from <= d['timestamp'] <= dt_upto
    ]
    
    # Создание DataFrame для удобства группировки
    df = pd.DataFrame(filtered_data)
    
    # Группировка данных в зависимости от типа агрегации
    if group_type == 'hour':
        grouped_data = df.groupby(pd.Grouper(key='timestamp', freq='H')).sum()
    elif group_type == 'day':
        grouped_data = df.groupby(pd.Grouper(key='timestamp', freq='D')).sum()
    elif group_type == 'month':
        grouped_data = df.groupby(pd.Grouper(key='timestamp', freq='M')).sum()
    
    # Создание полного списка меток времени в указанном интервале
    full_range = pd.date_range(start=dt_from, end=dt_upto, freq=group_type[0].upper())
    
    # Заполнение отсутствующих значений нулями и формирование ответа
    aggregated = grouped_data.reindex(full_range, fill_value=0)['value'].tolist()
    labels = [label.strftime('%Y-%m-%dT%H:%M:%S') for label in full_range]
    
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

