import pandas as pd
import numpy as np
import requests
import datetime as dt
from zipfile import ZipFile
from io import BytesIO

import moex_functions as mfunc

from url_reader import read_url, read_url_loop, url_processed, loop_processed

urls = {'description': 'https://iss.moex.com/iss/securities/%(ticker)s',
        'securities' : 'https://iss.moex.com/iss/securities',
        'bonds_list': 'https://iss.moex.com/iss/engines/stock/markets/bonds/',
        'market_data': 'https://iss.moex.com/iss/engines/%(engine)s/markets/%(market)s/boards/%(board)s/securities/%(ticker)s',
        'history_yields': 'https://iss.moex.com/iss/history/engines/%(engine)s/markets/%(market)s/boards/%(board)s/yields/%(ticker)s',
        'bondization': 'https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/bondization/%(ticker)s',
        'history_dividends': 'https://iss.moex.com/iss/securities/%(ticker)s/dividends',
        'history': 'https://iss.moex.com/iss/history/engines/%(engine)s/markets/%(market)s/boards/%(board)s/securities/%(ticker)s',
        'zcyz' : 'https://iss.moex.com/iss/engines/stock/zcyc/',
        'candles' : 'http://iss.moex.com/iss/engines/%(engine)s/markets/%(market)s/boards/%(board)s/securities/%(ticker)s/candles',
        'indices': 'https://iss.moex.com/iss/statistics/engines/stock/markets/index/analytics/%(ticker)s',
        'indices_collections': 'https://iss.moex.com/iss/securitygroups/stock_index/collections/%(ticker)s'}

class Moex:
    session = requests.Session()
    
    def __init__(self, ticker):
        self.session = Moex.session
        self.ticker = ticker

    def get_board(self):
        '''Returns list of stock exchange boards for a ticker'''
        kwargs = {'ticker': self.ticker,
                  'iss.meta': 'on',
                  'limit': 'unlimited'}
        url = urls['description'] + '.json'
        response = read_url(url=url,
                            **kwargs).json()['boards']
        try:
            return [x[1] for x in response['data'] if x[14] == 1][0]
        except:
            raise ValueError(f'ERROR {self.ticker} - unknown ticker')
    
    def get_market(self):
        '''Returns list of stock exchange boards for a ticker'''
        kwargs = {'ticker': self.ticker,
                  'iss.meta': 'on',
                  'limit': 'unlimited'}
        url = urls['description'] + '.json'
        response = read_url(url=url,
                          session=self.session,
                          **kwargs).json()['boards']
        try:
            return [x[5] for x in response['data'] if x[14] == 1][0]
        except:
            raise ValueError(f'ERROR {self.ticker} - unknown ticker')

    
    def get_engine(self):
        '''Returns list of stock exchange boards for a ticker'''
        kwargs = {'ticker': self.ticker,
                  'iss.meta': 'on',
                  'limit': 'unlimited'}
        url = urls['description'] + '.json'
        response = read_url(url=url,
                          session=self.session,
                          **kwargs).json()['boards']
        try:
            return [x[7] for x in response['data'] if x[14] == 1][0]
        except:
            raise ValueError(f'ERROR {self.ticker} - unknown ticker')

    
    def get_description(self, param=None):
        '''
        Return a dataframe with description of a ticker
        - param (str, None)
        - if param is not indicated all available parameters will be returned as DataFrame
        - if param is not in list - "None" will be returned
        'secid': 'Код ценной бумаги' \n
        'name': 'Полное наименование' \n
        'shortname': 'Краткое наименование' \n
        'isin': 'ISIN код' \n
        'issuesize': 'Объем выпуска' \n
        'facevalue': 'Номинальная стоимость' \n
        'faceunit': 'Валюта номинала' \n
        'issuedate': 'Дата начала торгов' \n
        'latname': 'Английское наименование' \n
        'listlevel': 'Уровень листинга' \n
        'isqualifiedinvestors': 'Бумаги для квалифицированных инвесторов' \n
        'typename': 'Вид/категория ценной бумаги' \n
        'group': 'Код типа инструмента' \n
        'type': 'Тип бумаги' \n
        'groupname': 'Типа инструмента' \n        
        'emitter_id': 'Код эмитента' \n
        'deliverytype': 'Исполнение' \n
        'frsttrade': 'Начало обращения' \n
        'lsttrade': 'Последний день обращения' \n
        'lstdeldate': 'Дата исполнения' \n
        'assetcode': 'Код базового актива' \n
        'exectype': 'Тип контракта' \n
        'lotsize': 'Лот' \n
        'contractname': 'Наименование контракта' \n
        'grouptype': 'Группа контрактов' \n
        'unit': 'Котировка' \n
        'matdate': 'Дата погашения' \n
        'initialfacevalue': 'Первоначальная номинальная стоимость' \n
        'programregistrynumber': 'Государственный регистрационный номер программы облигации' \n
        'earlyrepayment': 'Возможен досрочный выкуп' \n
        'daystoredemption': 'Дней до погашения' \n
        'couponfrequency': 'Периодичность выплаты купона в год' \n
        'coupondate': 'Дата выплаты купона' \n
        'couponpercent': 'Ставка купона, %' \n
        'couponvalue': 'Сумма купона, в валюте номинала'
        '''
        kwargs = {'ticker': self.ticker,
                  'iss.meta': 'on',
                  'limit': 'unlimited'}
        table = 'description'
        url = urls[table] + '.json'
        df = url_processed(url, table = table, **kwargs)
        df['name'] = [x.lower() for x in df['name']]
        if param:
            try:
                var_type = df.query(f'name == "{param}"')['type'].values[0]
                var = df.query(f'name == "{param}"')['value'].values[0]
                var = mfunc.convert_variable(var, var_type)
                return var
            except:
                print(f'Unknown param for a {self.ticker}')
                return None
        else:
            return df[['name', 'title', 'value']]
        
    def _parse_market_data(self):
        """
        Parses market data for a given ticker.
        Returns:
            pd.Series: A pandas Series containing the combined market data.
            If no data is available or an error occurs, an empty list is returned.
        """
        kwargs = {'ticker': self.ticker,
                  'iss.meta': 'on',
                  'limit': 'unlimited',                  
                  'engine' : self.get_engine(),
                  'market' : self.get_market(),
                  'board' : self.get_board()}
        url = urls['market_data'] + '.json'
        response = read_url(url=url,
                        session=self.session,
                        **kwargs).json()
        table = []
        for t in ['securities', 'marketdata_yields', 'marketdata']:
            try:
                info = pd.Series(response[t]['data'][0], index=response[t]['columns'])
                table.append(info)
            except:
                pass
        try:
            table = pd.concat(table)
            table = table[~table.index.duplicated(keep="first")]
        except:
            return []
        return table
        
    def _parse_history_yields(self, date_from: str = None, date_to: str = None):
        """
        Parses historical yield data for a given ticker within a specified date range.

        Args:
            date_from (str, optional): The start date for the data retrieval in 'YYYY-MM-DD' format. 
                                       Defaults to 30 days before the current date if not provided.
            date_to (str, optional): The end date for the data retrieval in 'YYYY-MM-DD' format. 
                                     Defaults to the current date if not provided.
        Returns:
            pd.DataFrame: A DataFrame containing the historical yield data with appropriate columns and data types.
        """
        date_from = (dt.datetime.now() - dt.timedelta(days=30)).date() if date_from == None else date_from
        date_to = dt.datetime.now().date() if date_to == None else date_to
        table = 'history_yields'
        kwargs = {
        'ticker': self.ticker,
        'iss.meta': 'on',
        'limit': 'unlimited',    
        'engine' : self.get_engine(),
        'market' : self.get_market(),
        'board' : self.get_board(),
        'date_from': date_from,
        'date_to': date_to,
        'table': table}
        url = urls[table] + '.json'
        response = read_url_loop(url, **kwargs)
        meta = response[0][table]['metadata']
        cols = response[0][table]['columns']
        data = [x for sub in [x[table]['data'] for x in response] for x in sub]
        df = pd.DataFrame(data, columns=cols)
        df = mfunc.make_new_types(df, meta)
        return df
    
    def _parse_history_results(self, date_from: str = None, date_to: str = None):
        """
        Parses historical trading data for a given ticker within a specified date range.

        Args:
            date_from (str, optional): The start date for the data retrieval in 'YYYY-MM-DD' format. 
                                       Defaults to 30 days before the current date if not provided.
            date_to (str, optional): The end date for the data retrieval in 'YYYY-MM-DD' format. 
                                     Defaults to the current date if not provided.
        Returns:
            pd.DataFrame: A DataFrame containing the historical yield data with appropriate columns and data types.
        """
        date_from = (dt.datetime.now() - dt.timedelta(days=30)).date() if date_from == None else date_from
        date_to = dt.datetime.now().date() if date_to == None else date_to
        table = 'history'
        kwargs = {
        'ticker': self.ticker,
        'iss.meta': 'on',
        'limit': 'unlimited',    
        'engine' : self.get_engine(),
        'market' : self.get_market(),
        'board' : self.get_board(),
        'date_from': date_from,
        'date_to': date_to,
        'table': table}
        url = urls[table] + '.json'
        response = read_url_loop(url, **kwargs)
        meta = response[0][table]['metadata']
        cols = response[0][table]['columns']
        data = [x for sub in [x[table]['data'] for x in response] for x in sub]
        df = pd.DataFrame(data, columns=cols)
        df = mfunc.make_new_types(df, meta)
        return df
    
    def get_offers(self):
        '''Returns a schedule of offers for a bond:'''
        kwargs = {'ticker': self.ticker,
        'iss.meta': 'on',
        'limit': 'unlimited',  
        'engine' : self.get_engine(),
        'market' : self.get_market(),
        'board' : self.get_board(),
        'table': 'offers'}
        url = urls['bondization'] + '.json'
        try:
            df = url_processed(url, **kwargs)
            return df
        except:
            return []
    
    def get_offer_date(self):
        '''Returns a next offer date of a bond in DateTime format'''
        df = self.get_offers()
        df = df.loc[~df['offertype'].str.contains('отмен')].reset_index(drop=True)
        try:
            return df.loc[df.offerdateend >= pd.to_datetime(dt.datetime.today()).normalize(), 'offerdateend'].iloc[0]
        except:
            return pd.NaT

    def get_price(self, date: str = None, type :str = 'last'):
        """
        Returns a last clean price for a ticker.
        If today is a trading day, it returns the latest transaction price or the last known transaction price if there have been no transactions.
        If today is not a trading day, it returns the last known price from the previous day.
        
        Parameters:
            date (str or None): A string representing the date or None. If provided, it considers the specified date; otherwise, it uses the current date.
            type (last or wap): The type of price to be considered. Options include the transaction price and the weighted average price. Default: last.
        """
        if date == None or pd.to_datetime(date).date() == dt.datetime.now().date():
            data = self._parse_market_data()
        else:
            data = self._parse_history_results(date_from = (pd.to_datetime(date) - dt.timedelta(days=30)).date(),
                                                date_to = pd.to_datetime(date).date())
            if len(data) > 0:
                data = data.iloc[-1]
            else:
                return []
        data.rename(index={'CLOSE' : 'LAST_1',
                            'LCURRENTPRICE' : 'LAST_2',
                            'LEGALCLOSEPRICE' : 'LAST_3'}, inplace=True, errors='ignore')
        data = data.reindex(['LAST', 'LAST_1', 'LAST_2', 'LAST_3', 'WAPRICE'])
        if self.get_engine() == 'futures':
            lp = data[data[['LAST', 'LAST_1', 'LAST_2', 'LAST_3']].first_valid_index()]
            wap = data[data[['LAST', 'LAST_1', 'LAST_2', 'LAST_3']].first_valid_index()]
        else:
            lp = data[data[['LAST', 'LAST_1', 'LAST_2', 'LAST_3']].first_valid_index()]
            wap = data['WAPRICE']
        if type == 'last':
            return lp
        return wap
            
    def get_ticker_currency(self):
        '''Returns currency name ticker is traded in'''
        currency = self.get_description(param = 'faceunit')
        if currency:
            currency = currency.replace('SUR' , 'RUB')
            return currency
        else:
            return None
    
    def get_dividends(self):
        '''Returns a list of known dividends for a ticker'''
        try:
            kwargs = {'ticker': self.ticker,
            'engine' : self.get_engine(),
            'market' : self.get_market(),
            'board' : self.get_board(),
            'table': 'dividends'}
            url = urls['history_dividends'] + '.json'
            df = url_processed(url, **kwargs)
            return df
        except: 
            print('no_data_for : ', self.ticker)
            return None

    def get_trade_currency(self):
        '''Returns list of stock exchange boards for a ticker'''
        kwargs = {'ticker': self.ticker}
        url = urls['description'] + '.json'
        response = read_url(url=url,
                            session=self.session,
                            **kwargs).json()['boards']
        try:
            cur = {key : value for key, value in zip(response['columns'], [i for subset in [s for s in response['data'] if s[response['columns'].index('is_primary')] == 1] for i in subset])}
            return cur.get('currencyid')            
        except:
            raise ValueError(f'ERROR {self.ticker} - unknown ticker')     
        
    def get_coupons(self):        
        '''Returns a schedule of coupon paymnets for a bond:'''
        kwargs = {
        'ticker': self.ticker,
        'iss.meta': 'on',
        'limit': 'unlimited',
        'engine': self.get_engine(),
        'market': self.get_market(),
        'board': self.get_board(),
        'table': 'coupons'}
        url = urls['bondization'] + '.json'
        df = url_processed(url, **kwargs)
        # костыль для ошибок данных ММВБ когда есть строки с несуществующими купонами
        df['valueprc'] = df.apply(lambda x: round(x['value'] / x.facevalue * (365 / (x.coupondate - x.startdate).days), 4) * 100
        if pd.isnull(x.valueprc) == True and pd.isnull(x.value) == False else x.valueprc, axis=1)
        # # костыль для пустого первого купона флоатера
        if np.isnan(df['valueprc'][0]):
            coupon = self._parse_market_data()['ACCRUEDINT']
            sd = df['startdate'][0]
            ed = (dt.datetime().now() + dt.timedelta(days=30)).date()
            coup_days = (ed - sd).days
            coup = (coupon / coup_days * 365) / df['facevalue'][0] * 100
            coup_sum = coup/100 * \
            df['facevalue'][0] / \
            (365 / (df['coupondate'][0] - df['startdate'][0]).days)
            df.loc[0, 'valueprc'] = round(coup, 2)
            df.loc[0, 'value'] = round(coup_sum, 2)
        return df
    
    def get_offers(self):
        '''Returns a schedule of offers for a bond:'''
        kwargs = {'ticker': self.ticker,
        'engine' : self.get_engine(),
        'market' : self.get_market(),
        'board' : self.get_board(),
        'table': 'offers'}
        url = urls['bondization'] + '.json'
        df = url_processed(url, **kwargs).rename(columns={'price': 'offerprice'})
        df.fillna({'offerprice': 100}, inplace=True)
        return df

    def get_amortization(self):
        '''Returns schedule of redemtion paymnets for a bond:'''
        kwargs = {'ticker': self.ticker,
        'engine' : self.get_engine(),
        'market' : self.get_market(),
        'board' : self.get_board(),
        'table': 'amortizations'}
        url = urls['bondization'] + '.json'
        df = url_processed(url, **kwargs)
        return df
    
    def get_bond_schedule(self, till_offer: bool = False, last_coupon: bool = False):
        '''Returns an unprocessed schedule of payments for a bond:
        - coupons
        - amortization payments
        - offers'''
        try:
            coupons = self.get_coupons()
            try:
                if pd.isnull(coupons.loc[coupons['coupondate'] > dt.datetime.now(), 'valueprc'].iloc[0]) == True:        
                    acc_int = self._parse_market_data()['ACCRUEDINT']
                    set_date = pd.to_datetime(self._parse_market_data()['SETTLEDATE'])
                    c_str = coupons.loc[coupons['coupondate'] > dt.datetime.now()].iloc[0]['startdate'] 
                    c_end = coupons.loc[coupons['coupondate'] > dt.datetime.now()].iloc[0]['coupondate'] 
                    f_v = coupons.loc[coupons['coupondate'] > dt.datetime.now()].iloc[0]['facevalue'] 
                    coup_rate = round(acc_int / f_v * 365 / round(((set_date - c_str).days),0),3)*100
                    coup_sum = round((coup_rate / 100 * f_v) *  round((c_end - c_str).days ) / 365,2)
                    coupons.loc[coupons.loc[coupons['coupondate'] > dt.datetime.now()].index[0], 'valueprc'] = coup_rate
                    coupons.loc[coupons.loc[coupons['coupondate'] > dt.datetime.now()].index[0], 'value'] = coup_sum
            except:
                pass
            offers = self.get_offers()
            amortization = self.get_amortization()
            # coupons processing
            coupons.drop(columns={'issuevalue', 'primary_boardid',
                                  'recorddate', 'startdate', 'value_rub'}, inplace=True, errors='ignore')
            coupons.rename(columns={'coupondate': 'date',
                                    'valueprc': 'couponprc',
                                    'value': 'couponvalue'}, inplace=True)
            coupons['operationtype'] = 'coupon'
            # offers processing
            offers = offers.loc[~offers['offertype'].str.contains('отмен')].reset_index(drop=True)
            offers.drop(columns={'issuevalue', 'offerdatestart', 'offerdate', 'primary_boardid',
                        'agent', 'offertype', 'value'}, inplace=True, errors='ignore')
            offers.rename(columns={'offerdateend': 'date'}, inplace=True)
            offers['operationtype'] = 'offer'
            # amortization processing
            amortization.drop(columns={
                'issuevalue', 'primary_boardid', 'value_rub'}, inplace=True, errors='ignore')
            amortization.rename(columns={'amortdate': 'date',
                                         'data_source': 'operationtype',
                                         'valueprc': 'redemptionprc',
                                         'value': 'redemptionvalue'}, inplace=True)
            if amortization.loc[amortization.index[-1], 'operationtype'] == 'amortization':
                amortization.loc[amortization.index[-1], 'operationtype'] = 'maturity' 
            issue_date = {'date': self.get_description(
                param='issuedate'), 'operationtype': 'issue'}
            # concatenate issue date / coupons / offers / amortization schedules
            df = pd.concat([coupons, offers, amortization], ignore_index=True)
            df.loc[len(df)] = issue_date
            # sorting by date and operation type (coupon - last) to backfill nan coupon rates
            mapping = {'issue': 0,
                       'coupon': 4,
                       'offer': 1,
                       'amortization': 2,
                       'maturity': 3}
            df['order'] = df['operationtype'].map(mapping)
            df = df.sort_values(by=['date', 'order']).reset_index(drop=True)
            for column in ['couponprc']:
                df[column] = df[column].bfill()
            df['order'] = df['operationtype'].map(mapping)
            df = df.sort_values(by=['date', 'order'])
            df = df.drop(columns=['order']).reset_index(drop=True)
            # final sorting by date and operation type
            mapping = {'issue': 0,
                       'coupon': 1,
                       'offer': 2,
                       'amortization': 3,
                       'maturity': 4}
            df['order'] = df['operationtype'].map(mapping)
            df = df.sort_values(by=['date', 'order']).reset_index(drop=True)
            df = df.drop(columns=['order'])
            for column in ['isin', 'name', 'initialfacevalue', 'faceunit', 'secid']:
                df[column] = df[column].fillna(df[column].mode()[0])
            df.loc[0, 'facevalue'] = df.loc[0, 'initialfacevalue']
            df.loc[0, 'couponvalue'] = 0
            for ix in range(1, len(df)):
                if df.loc[ix-1, 'operationtype'] == 'amortization':
                    df.loc[ix, 'facevalue'] = df.loc[ix-1, 'facevalue'] - \
                        df.loc[ix-1, 'redemptionvalue']
                else:
                    df.loc[ix, 'facevalue'] = df.loc[ix-1, 'facevalue']
            if last_coupon:
                df['couponprc'] = df['couponprc'].ffill()
            # calculating coupon values as (days from previous coupon date to current date) * (coupon rate) * (face value)
            df['couponvalue'] = df['couponvalue'].fillna((df['date']
                                                          - df['date'].shift()).dt.days / 365 * df['couponprc'] / 100 * df['facevalue'])
            df['couponvalue'] = round(df['couponvalue'], 2)
            # setting coupon and redemption values to 0 after the next offer date
            if till_offer:
                offer_index = df.loc[(df['operationtype'] == 'offer') & (
                    df['date'] >= dt.datetime.now())].index
                if offer_index.empty:
                    pass
                else:
                    offer_index = offer_index[0]
                    df.loc[offer_index, 'redemptionvalue'] = df.loc[offer_index,
                                                                    'offerprice'] / 100 * df.loc[offer_index, 'facevalue']
                    df.loc[offer_index+1:, ['couponvalue', 'redemptionvalue']] = 0
                    df.loc[(df['operationtype'] == 'offer') & (df['date'] < dt.datetime.now()),
                            ['couponvalue', 'redemptionvalue']] = 0
            else:
                df.loc[(df['operationtype'] == 'offer'), [
                    'couponvalue', 'redemptionvalue']] = 0
            df['couponvalue'] = [np.nan if pd.isnull(
                x) == True else y for x, y in zip(df['couponprc'], df['couponvalue'])]
            df.fillna({'redemptionvalue': 0}, inplace=True)
            df.fillna({'offerprice': 0}, inplace=True)
            df.fillna({'redemptionprc': 0}, inplace=True)
            return df
        except:
            return pd.DataFrame()        
    
    def find_ticker(self, type: str = None):
        """
        Finds and returns ticker information from the MOEX ISS API.
        Args:
            type (str, optional): The type of security to filter by. 
                                  Can be 'share', 'bond', 'index', or 'future'. 
                                  Defaults to None.
        Returns:
            DataFrame: A pandas DataFrame containing the filtered ticker information 
                       sorted by 'shortname'. Returns None if an error occurs.
        """
        try:
            kwargs = {
                'iss.meta': 'on',
                'limit': 'unlimited',
                'table': 'securities',
                'search_ticker' : self.ticker}
            url = 'https://iss.moex.com/iss/securities' + '.json'
            df = url_processed(url, **kwargs)
            if type in ['share', 'bond', 'index', 'future']:
                return df[df['group'].str.contains(type)].loc[df['is_traded'] == 1].sort_values(by='shortname')
            else:
                return df.loc[df['is_traded'] == 1].sort_values(by='shortname')
        except: 
            return None       

    @staticmethod
    def get_bonds_list():
        '''Returns a table of all traded bonds at the current time.'''
        sec = url_processed(urls['bonds_list'] + 'securities' + '.json',
                            **{'meta': 'on', 'table': 'securities'}).drop(columns='DURATION', errors='ignore')
        md = url_processed(urls['bonds_list'] + 'securities' + '.json',
                           **{'meta': 'on', 'table': 'marketdata'}).drop(columns='DURATION', errors='ignore')
        mdy = url_processed(urls['bonds_list'] + 'securities' + '.json',
                            **{'meta': 'on', 'table': 'marketdata_yields'})
        [['SECID',
          'BOARDID',
          'PRICE',
          'EFFECTIVEYIELD',
          'YIELDDATETYPE',
          'DURATION',
          'ZSPREADBP',
          'GSPREADBP']]
        b_list = sec.merge(md, how='left', on=['SECID', 'BOARDID']).merge(
            mdy, how='left', on=['SECID', 'BOARDID'])
        board_groups = [58, 193, 207, 245]

        boards = read_url(urls['bonds_list'] + 'boards' + '.json',
                          **{'meta': 'off', 'table': 'boards'}).json()['boards']['data']
        boards = [b[2] for b in boards if b[1] in board_groups]
        b_list = b_list.loc[b_list.BOARDID.isin(boards)]
        return b_list
    
    def get_candles(self,
                    date_from: str = None,
                    date_to: str = None,
                    interval : int = 24):
        """
        Retrieves candle data for a given ticker within a specified date range and interval.

        Args:
            date_from (str, optional): The start date for the data retrieval in 'YYYY-MM-DD' format. 
                                       Defaults to 30 days before the current date if not provided.
            date_to (str, optional): The end date for the data retrieval in 'YYYY-MM-DD' format. 
                                     Defaults to the current date if not provided.
            interval (int, optional): The interval for the candle data. Default is 24.

        Returns:
            pd.DataFrame: A DataFrame containing the candle data.
        """
        date_from = (dt.datetime.now() - dt.timedelta(days=30)).date() if date_from == None else date_from
        date_to = dt.datetime.now().date() if date_to == None else date_to
        table = 'candles'
        kwargs = {
        'ticker': self.ticker,
        'iss.meta': 'on',
        'limit': 'unlimited',    
        'engine' : self.get_engine(),
        'market' : self.get_market(),
        'board' : self.get_board(),
        'date_from': date_from,
        'date_to': date_to,
        'table': table,
        'interval' : interval}
        url = urls[table] + '.json'
        df = url_processed(url, **kwargs)
        return df

    @staticmethod
    def get_indices_groups(name: str = None):
        """
        Returns a list of indice groups (e.g., "stock_index_bonds").
        Args:
        - name (str, optional): Name of indice group to return a detailed list of indices in a group. \n
        Example:
        Moex.get_indices_groups(name='stock_index_bonds_corporate')
        """
        if name:
            kwargs = {
            'ticker': name,
            'iss.meta': 'on',
            'limit': 'unlimited',    
            'table': 'securities'}
            data = url_processed(
                url=urls['indices_collections'] + '/securities' + '.json', **kwargs)
            return data
        kwargs = {
        'ticker': '',
        'iss.meta': 'on',
        'limit': 'unlimited',    
        'table': 'collections'}
        data = url_processed(url=urls['indices_collections'] + '.json', **kwargs)
        return data

    @staticmethod
    def get_indice_tickers(ticker):
        '''Returns a list of tickers in an indice and weights.'''
        kwargs = {
        'ticker': ticker,
        'iss.meta': 'on',
        'limit': 'unlimited',    
        'table': 'analytics'}
        df = url_processed(urls['indices'] + '.json', **kwargs)
        return df

    @staticmethod
    def get_zcurve_params(date = None):
        df = read_url(urls['zcyz'] + '.json', date=date).json()
        min_date = pd.to_datetime(df['params.dates']['data'][0][0]) 
        if date == None:
            date = pd.to_datetime('today').normalize()
        if pd.to_datetime(date) < min_date:
            print('error, min date: ', min_date.date())
            return None  
        df = df['params']
        zcyz_params = pd.DataFrame(df['data'], columns=df['columns'])
        zcyz_params.columns = zcyz_params.columns.str.lower()
        zcyz_params = zcyz_params.rename(columns={'b1': 'b0', 'b2': 'b1', 'b3': 'b2'})
        return zcyz_params

    @staticmethod
    def get_zcurve_params_history():
        """
        Fetches and processes the zero-coupon yield curve parameters history from the Moscow Exchange.
        Returns:
            pd.DataFrame: A DataFrame containing the processed zero-coupon yield curve parameters history.
        """
        url = "http://moex.com/iss/downloads/engines/stock/zcyc/dynamic.csv.zip"
        response = read_url(url)
        with ZipFile(BytesIO(response.content)) as zip_file:
            with zip_file.open(zip_file.namelist()[0]) as file:
                df = pd.read_csv(file, sep=';', decimal=',', skiprows=1)
                df.columns = df.columns.str.lower()
                df = df.rename(columns={'b1': 'b0', 'b2': 'b1', 'b3': 'b2'})
                df['tradedate'] = pd.to_datetime(df['tradedate'], format='%d.%m.%Y')
        return df

    @staticmethod
    def get_zcurve_prices(date = None):
        """
        Fetches and returns zero-coupon yield curve prices for a given date.

        Parameters:
            date (str or None): The date for which to fetch the yield curve prices. 
                                If None, the current date is used. The date should be in a format 
                                recognized by pandas.to_datetime.
        Returns:
                DataFrame or None: A DataFrame containing the zero-coupon yield curve prices or None if an error occurs.
        """
        df = read_url(urls['zcyz'] + '.json', date=date).json()
        min_date = pd.to_datetime(df['params.dates']['data'][0][0]) 
        if date == None:
            date = pd.to_datetime('today')   
        if pd.to_datetime(date) < min_date:
            print('error, min date: ', min_date.date())
            return None  
        df = df['yearyields']
        df = pd.DataFrame(df['data'], columns=df['columns'])
        df.columns = df.columns.str.lower()
        df['tradedate'] = pd.to_datetime(df['tradedate'])
        return df

    @staticmethod
    def calculate_zyield(p, t):
        """
        Calculate the yield based on given parameters.

        Parameters:
            p (DataFrame): A pandas DataFrame containing the necessary financial data.
            t (float): A time parameter used in the calculation.

        Returns:
            float: The calculated yield.
        """
        def func_a(k):
            s = [0, 0.6]
            for i in range(2,9):
                next = s[-1] + s[1]*k**(i-1)
                s.append(next)
            return s
        k = 1.6
        a = func_a(k)
        b = [0.6*k**i for i in range(0,9)]
        g = p.iloc[-9:]
        dl = sum([g.iloc[i] * np.exp( -(t - a[i])**2 / b[i]**2 ) for i in range(0,9)])
        dr = p.b0 + (p.b1 + p.b2) * (p.t1 / t) * (1- np.exp(-t/p.t1)) - p.b2 * np.exp(-t/p.t1)
        g_t = dl + dr
        y_t = 10000 * (np.exp(g_t/10000) -1)
        d_t = 1 / (1+ y_t/10000)**t
        return y_t / 100

    @staticmethod
    def get_zyield_for_maturity(t: float, date: str = None):
        """
        Calculates the zero-coupon yield for a given maturity.

        Args:
            t (float): The maturity in years.
            date (str, optional): The date for which to calculate the yield. Defaults to None.

        Returns:
            float: The calculated zero-coupon yield.
        """
        p = Moex.get_zcurve_params(date=date)
        if p is None or p.empty:
            print(f'No data for {date}')
            return None
        p = pd.Series(p.iloc[0], index=p.columns)
        return Moex.calculate_zyield(p, t)