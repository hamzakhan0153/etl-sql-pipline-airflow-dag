import requests
from datetime import date, datetime, timedelta



def extract_data_from_source():

    data = []

    get_states_url = 'https://gist.githubusercontent.com/mshafrir/2646763/raw/8b0dbb93521f5d6889502305335104218454c2bf/states_hash.json'

    states_data = requests.get(get_states_url)

    states_list = states_data.json()

    for state in states_list.keys():
        print(state)
        url = 'https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/'

        headers = {
        'User-Agent' : "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
        time_delta=30
        max_date = '2024-08-31'
        min_date = (date.today() - timedelta(days=time_delta)).strftime("%Y-%m-%d")
        params = {
        "size": 500,
        "date_received_max": max_date,
        "Time": min_date,
        "state": state
        }

        api_data = requests.get(url=url,headers=headers,params=params)
        raw  = api_data.json()
        if 'hits' in raw and 'hits' in raw['hits']:
            print(raw['hits']['hits'])
            records = raw['hits']['hits']
            data.extend(records)        
    return data