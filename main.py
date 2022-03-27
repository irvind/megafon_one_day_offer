import random
from datetime import date, time, datetime, timedelta

import pandas as pd
import numpy as np


def generate_clients_dataframe() -> pd.DataFrame:
    '''
    Генерация датафрейма с данными клиента
    '''
    cities = ['Москва', 'Санкт-Петербург', 'Казань', 'Волгоград']
    tarifs = list(range(5))

    data = []
    for i in range(30):
        created_at = datetime(2000, 1, 1) + timedelta(days=np.random.randint(0, 22*356))    
        data.append([
            i + 1,
            np.random.random() * 500,
            created_at.date(),
            np.random.randint(18, 70),
            random.choice(cities),
            random_datetime(created_at, datetime.now()),
            random.choice(tarifs)
        ])

    df = pd.DataFrame(data, columns=[
        'id', 'balance', 'created_at_date', 'age', 'city', 'last_active_datetime', 'current_tariff'
    ])

    return df


def generate_events_dataframe(clients_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Генерация датафрейма, содержащего события клиента (звонки, смс, интернет-траффик)
    '''
    data = []
    for _, client_row in clients_df.iterrows():
        client_data = generate_events_for_client(
            client_row['id'],
            datetime.combine(client_row['created_at_date'], time(0, 0))
        )
        data.extend(client_data)

    df = pd.DataFrame(
        data=[
            [idx] + row for idx, row in enumerate(data, start=1)
        ],
        columns=('id', 'timestamp', 'timestamp_date', 'client_id', 'type', 'amount')
    )

    return df


def generate_events_for_client(client_id: int, min_datetime: datetime) -> datetime:
    '''
    Генерируем данные по событиям относящимся к одному клиенту
    '''
    data = []
    now = datetime.now()
    for i in range(np.random.randint(20, 40)):
        timestamp_dt = random_datetime(min_datetime, now)
        timestamp_date = timestamp_dt.date()
        item = [timestamp_dt, timestamp_date, client_id]
        if np.random.rand() > 0.2:
            data.append(item + ['звонок', np.random.randint(1, 5)])
        if np.random.rand() > 0.4:
            data.append(item + ['смс', np.random.randint(1, 3)])
        if np.random.rand() > 0.5:
            data.append(item + ['интернет', np.random.randint(1, 200)])    

    return data


def random_datetime(start: datetime, end: datetime) -> datetime:
    '''
    Генерирует случайный datetime между start и end
    '''
    diff = end - start
    seconds_diff = (diff.days * 24 * 60 * 60) + diff.seconds
    return start + timedelta(seconds=np.random.randint(0, seconds_diff))


def create_report(events_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Генерируем подневный отчет для событий клиентов
    '''
    result = events_df.groupby(['timestamp_date', 'client_id', 'type'])[['amount']].sum()
    data = {}
    mapping = {'звонок': 'call', 'смс': 'sms', 'интернет': 'internet'}
    for idx, row in result.iterrows():
        timestamp_date, client_id, _type = idx
        amount = row['amount']
        key = (timestamp_date, client_id)
        if key not in data:
            data[key] = {'call': 0, 'sms': 0, 'internet': 0}
        data[key][mapping[_type]] = amount
    
    df = pd.DataFrame(
        [[k[1], k[0], v['call'], v['sms'], v['internet']] for k, v in data.items()],
        columns=('client_id', 'date', 'call', 'sms', 'internet')
    )

    return df


if __name__ == '__main__':
    clients_df = generate_clients_dataframe()
    events_df = generate_events_dataframe(clients_df)
    report_df = create_report(events_df)
    print(report_df)
