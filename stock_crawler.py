import pandas as pd
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup as bs
import time
import requests
import json
from pandas.io.json import json_normalize
from datetime import datetime


def try_urlopen(url):
    resp = None
    retry = False
    success = False

    for i in range(10):
        try:
            if retry:
                print('connection retry:', url)
            req = Request(url)
            resp = urlopen(req)
        except ConnectionResetError:
            time.sleep(5)
            print('connection reset:', url)
            retry = True
            pass
        except Exception as e:
            print('connection exception:', url)
            print(e)
            time.sleep(5)
            retry = True
            pass
        else:
            if retry:
                print('connection retry success:', url)
            success = True
        if success:
            break    
        
    return resp


def get_etf_ticker():
    result_list = []
    type_list = ['etf', 'etn']
    
    for t in type_list:        
        url = f'https://finance.naver.com/api/sise/{t}ItemList.nhn'
        print(url)
        json_data = json.loads(requests.get(url).text)
        df_ = json_normalize(json_data['result'][f'{t}ItemList'])
        df_ = (df_.loc[:, ['itemcode', 'itemname', 'marketSum']]
              .rename(columns={'marketSum': 'market_sum', 'itemcode': 'ticker'}))
        result_list.append(df_)
    
    df = pd.concat(result_list)            
    
    return df


def get_stock_price(ticker, timeframe, period):
    url = r'https://fchart.stock.naver.com/sise.nhn?symbol={tk}&timeframe={tf}&count={p}&requestType=0'.format(tk=ticker, tf=timeframe, p=period)
    print("processing: ", url)
    price_data = try_urlopen(url)
    #resp = try_urlopen(url)
    #price_data = resp.read()
    soup = bs(price_data)    
    item_list = soup.find_all('item')
    
    list_of_list = [item.get('data').split('|') for item in item_list]
    df_price = (pd.DataFrame(list_of_list,
                             columns=['tdate', 'open', 'high', 'low', 'close', 'volume']))
    df_price['ticker'] = ticker
    
    return df_price


def get_indexes():
    ticker_list_market = ['KOSPI', 'KOSDAQ']
    df_list = []

    for ticker in ticker_list_market:
        df = get_stock_price(ticker, 'day', 10000)
        df_list.append(df)

    df_market = pd.concat(df_list)
    columns_select = ['ticker', 'tdate', 'open', 'high', 'low','close', 'volume']
    df_market = df_market.loc[:, columns_select]
    
    return df_market


def get_global_indexes():
    config_SNP = {'ticker': 'SPI@SPX',
                  'itemname': 'S&P500'}
    config_NIKKEI = {'ticker': 'NII@NI225',
                     'itemname': 'Nikkei'}
    config_EURO = {'ticker': 'STX@SX5E',
                   'itemname': 'Eurostoxx'}
    config_CHINA = {'ticker': 'SHS@000001',
                    'itemname': 'Shanghai'}

    config_list = [config_SNP, config_NIKKEI, config_EURO, config_CHINA]
    print("config_list: , ", config_list)

    df_list = []
    column_select = ['symb', 'xymd', 'open', 'high', 'low', 'clos', 'gvol']

    s = time.time()
    for config in config_list:
        ticker = config.get('ticker')
        url = f'https://finance.naver.com/world/worldDayListJson.nhn?symbol={ticker}&fdtc=0&page='
        print("processing: ", ticker)
        result_list = []

        for i in range(1, 10001):
            url_ = url + str(i) 
            resp = json.load(try_urlopen(url_))            
            result_list.extend(resp)            

            if len(resp) < 10:
                break

        print("crawl completed: ", ticker)
        df = pd.DataFrame(result_list)
        df = (df.loc[:, column_select]
              .rename(columns={'xymd': 'tdate', 'symb': 'ticker', 'clos': 'close', 'gvol': 'volume'}))
        df_list.append(df)

    e = time.time()
    minute, second = divmod((e - s), 60)
    second = round(second, 0)
    print(f'crawl cost: {minute} min {second} sec')
    print('number of stocks: ', len(df_list))
    print('number of records: ', len(df_concat))
    
    df_market_global = pd.concat(df_list)    
    
    return df_market_global