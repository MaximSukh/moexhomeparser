import pandas as pd
import certifi
import requests
import moex_functions as mfunc

def read_url(url: str,
            session = requests.Session(),
            print_url : bool = False,
            meta: str = None,
            table: str = None,
            limit: str = None,
            date: str = None,
            date_from: str = None,
            date_to: str = None,
            interval: int = None,
            start: int = None,
            search_ticker : str = None,   
            **kwargs):
    params = {'iss.meta': meta,
            'iss.only': table,
            'limit': limit,
            'date': date,
            'from': date_from,
            'till': date_to,
            'interval': interval,
            'start': start,
            'q' : search_ticker}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0",
        "Accept-Encoding": "*",
        "Connection": "keep-alive"}
    url = url % kwargs
    response = session.get(url,
                        headers=headers,
                        params=params,
                        verify=certifi.where(),
                        json=True)
    if print_url:
        return print(response.url)
    return response

def read_url_loop(url, table : str, **kwargs):
    iter = 0
    start = 0
    data_list = []
    while True:
        try:
          response = read_url(url, table = table, start = start, **kwargs)          
          data_list.append(response.json())
          start += 100
          if len(response[table]['data']) == 0:
              break		
        except:
            iter += 1
            if iter > 5:
                break        
    return data_list

def loop_processed(url, table :str, **kwargs):
    response = read_url_loop(url, table, **kwargs)
    meta = response[0][table]['metadata']
    cols = response[0][table]['columns']
    data = [x for sub in [x[table]['data'] for x in response] for x in sub]
    df = pd.DataFrame(data, columns=cols)
    df = mfunc.make_new_types(df, meta)
    return df
   
def url_processed(url, table: str, **kwargs):
    response = read_url(url, **kwargs).json()[table]
    df = pd.DataFrame(response['data'], columns=response['columns'])
    df = mfunc.make_new_types(df, response['metadata'])
    return df



