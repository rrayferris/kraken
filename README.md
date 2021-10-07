# How to Use:

Retrieve OHLC Data from either the Kraken OHLC endpoint or the Trades endpoint.

For each, cd to ohlc/ directory and run main.py.

Include the following args:
- Identify the endpoint you are calling
- Currency that you are interested in
- Start of timeframe (YYYY-M-D HH:MM:SS)
- End of timeframe (YYYY-M-D HH:MM:SS)
- Interval ('1Min', '5Min', '15Min', '30Min', '1H', '4H', '24H',  '7D', '15D')
- Output filename 




## Example OHLC Endpoint



```bash
python -i main.py 'OHLC' 'XXBTZUSD' '2021-3-1 00:00:00' '2021-3-1 04:00:00' '4H' 'OHLC_analysis_file_mar2021_4H.parquet'

```

## Example Trade Endpoint

```bash
python -i main.py 'Trade' 'XXBTZUSD' '2021-3-1 00:00:00' '2021-3-1 04:00:00' '4H' 'Trade_analysis_file_mar2021_4H.parquet'

```

## Run build_dash.py
To see the result of main, run the following from ohlc/. This should open a browser with as many tabs as there are files in the directory:  ohlc/business_tables/

```bash
python build_dash.py
```

