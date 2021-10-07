from pandas._libs.tslibs.timedeltas import ints_to_pytimedelta
import requests
import json
import pandas as pd 
from datetime import datetime, date
import time
import sys 
import csv
import pdb



class Kraken():
    """ 
    Constructs methods to ingest, process, store, and export OHLC data for analysis, given a, 
    time-interval, and time period

    Args and Attributes:
        pair                    (str): Kracken Ticker - Default Arg is XXBTZUSD.
        start_date              (str): First Datetime you would like OHLC Data.
        end_date                (str): Last Dattime you would like OHLC Data
        interval                (int): Time-Interval for which you want data (Hour, Minute, Day)
        raw_file_name           (str): Raw file output
        intermediary_file_name  (str): Intermediary file output
        business_file_name      (str): Business File Output    
    """
    
    def __init__(self, pair, start_date, end_date, interval, raw_file_name, intermediary_file_name, business_file_name) -> None:
        self.pair = pair
        self.start_unix = int(time.mktime(start_date.timetuple()))
        self.end_unix = int(time.mktime(end_date.timetuple()))
        self.interval = interval
        self.raw_file_name = raw_file_name
        self.intermediary_file_name = intermediary_file_name
        self.business_file_name = business_file_name

    def get_ohlc(self, interval):
        """
        Input:  pair (ticker id), start_date, end_date
        Output: Returns aggregated OHLC data according to the selected interval, 
                plus additional metrics: (h+l+c/3, mean, and median
        
        Note:   This endpoint only returns up to 720 response per request, and if 
                the response for the requested timeframe will exceed 720, the response
                defaults to the most recent 720 instances. 

                If your timeframe - time interval combination will exceed 720, Kraken documentation 
                recommends constructing your own OHLC data using the Trades endpoint. For more
                information: https://bit.ly/3iDUja5
        """
        ohlc = pd.DataFrame()
        while self.start_unix < self.end_unix:
            url = f'https://api.kraken.com/0/public/OHLC?pair={self.pair}&interval={interval}&since={self.start_unix}'
            r = requests.get(url)
            if r.status_code == 200:
                ohlc_data = r.json()
                ohlc = ohlc.append(ohlc_data['result'][self.pair])
                self.start_unix = int(ohlc[0].max())
            else:
                return r.text
            time.sleep(2)
        ohlc.columns =['date', 'open', 'high', 'low', 'close', 'vwap', 'volume', 'tradecount']
        ohlc = ohlc.iloc[:-1 , :]
        return ohlc

    def clean_ohlc(self, data):
        """
        Input: Raw OHLC data from the OHLC Endpoint
        Output: Date + OHLC fields.
        """
        data['date'] = pd.to_datetime(data['date'], unit='s')
        data.drop(['vwap', 'volume', 'tradecount'], axis=1, inplace=True)
        return data 

    def calculate_ohlc_metrics(self, data):
        """
        Input: Clean, aggregated OHLC Data
        Output: OHLC Metrics
        """
        data[['low', 'high', 'open', 'close']] = data[['low', 'high', 'open', 'close']].apply(pd.to_numeric)
        data['hlc3'] = (data['low'] + data['high'] + data['close']) / 3
        data['mean'] = data.hlc3.rolling(24).mean()
        data['median'] = data.hlc3.rolling(24).median()
        return data

    def get_trades(self):
        """
        Input:  pair (ticker id), start_date, end_date
        Output: Dataframe with unix timestamp, pair cost, amount transacted (in btc), 
                bought/sold, order_type, and margin
        """
        
        trades = pd.DataFrame()
        while self.start_unix < self.end_unix:
            print(self.start_unix)
            url = f'https://api.kraken.com/0/public/Trades?pair={self.pair}&since={self.start_unix}'
            r = requests.get(url)
            if r.status_code == 200:
                ohlc_data = r.json()
                trades = trades.append(ohlc_data['result'][self.pair])
                self.start_unix = int(trades[2].max())
            else:
                return r.text
            time.sleep(2)
        trades.columns = ['price', 'amount', 'unix_date', 'transaction_type', 'long', 'unknown']    
        return trades 


    def clean_trade_data(self, trade_data):
        """ 
        Input:  raw trade data.
        Output: Datetime - date and pricing dataframe
        """
        trade_data['date'] = pd.to_datetime(trade_data['unix_date'],  unit='s')
        trade_data.drop(['unix_date', 'amount', 'transaction_type', 'long', 'unknown'], axis=1, inplace=True)
        return trade_data

    
    def aggregate_trade_data(self, data, interval):
        """
        Input:  Date - Price Data
        Output: Returns aggregated OHLC data according to the selected interval, 
                plus additional metrics: (h+l+c/3, mean, and median
        """

        data = data.reset_index(drop=True).set_index('date').resample(interval).agg(['min', 'max', 'first', 'last']).reset_index()
        # data.reset_index(inplace=True)
        data.columns = ['date', 'low', 'high', 'open', 'close']
        return data

