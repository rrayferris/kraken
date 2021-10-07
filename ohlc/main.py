from kraken.kccase import Kraken
from kraken import utils
import argparse
from datetime import datetime, date
import time

### python -i main.py 'Trades' 'XXBTZUSD' '2021-3-1 00:00:00' '2021-4-1 00:00:00' '4H' 'raw_file.parquet' 'int_file.parquet' 'OHLC_analysis_file_mar2021_2H.parquet' ###
### python -i main.py 'OHLC' 'XXBTZUSD' '2021-3-1 00:00:00' '2021-4-1 00:00:00' '4H' 'raw_file.parquet' 'int_file.parquet' 'OHLC_analysis_file_mar2021_2H.parquet' ###
parser = argparse.ArgumentParser(description='Collects details on how how extract and transform data for analysis')
parser.add_argument('endpoint', type=str, default='Trade')
parser.add_argument('pair', type=str, help='Valid Kraken Ticker', default='XXBTZUSD')
parser.add_argument('start', type=str, help='Beginning of Timeframe: "%Y-%m-%d %H:%M:%S" Formatting')
parser.add_argument('end', type=str, help='End of Timeframe: "%Y-%m-%d %H:%M:%S" Formatting')
parser.add_argument('interval', type=str, default='1H')
parser.add_argument('raw_data_file', type=str)
parser.add_argument('intermediary_data_file', type=str)
parser.add_argument('business_analysis_file', type=str)


args = parser.parse_args()
k = Kraken(args.pair, datetime.strptime(args.start, "%Y-%m-%d %H:%M:%S"), datetime.strptime(args.end, "%Y-%m-%d %H:%M:%S"), args.interval, args.raw_data_file, args.intermediary_data_file, args.business_analysis_file)
if args.endpoint == 'Trade':
    raw_trade_data = k.get_trades()
    clean_trade_data = k.clean_trade_data(raw_trade_data)
    clean_ohlc = k.aggregate_trade_data(clean_trade_data, args.interval)

if args.endpoint == 'OHLC':
    hlc_interval = utils.transform_interval(args.interval)
    ohlc_raw = k.get_ohlc(hlc_interval)
    clean_ohlc = k.clean_ohlc(ohlc_raw)

final_trade_data = k.calculate_ohlc_metrics(clean_ohlc)
final_trade_data.to_parquet(f'business_tables/{args.business_analysis_file}')


 
