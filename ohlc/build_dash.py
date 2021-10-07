import plotly.graph_objects as go
from dash import dcc
import pandas as pd
from pathlib import Path


def build_plot(file, plot_title):
    data = pd.read_parquet(file)
    fig = go.Figure(data=[go.Candlestick(x=data['date'],
        open=data['open'],
        high=data['high'],
        low=data['low'],
        close=data['close']), 
    go.Scatter(x=data.date, y=data['mean'], line=dict(color='orange', width=1),name='Mean Daily Price'),
    go.Scatter(x=data.date, y=data['median'], line=dict(color='green', width=1), name='Median Daily Price')])

    fig.update_layout(
        width=1000, height=800,
        title=plot_title,
        yaxis_title='BTC Cost in $USD',
        xaxis_title='DateTime')
    return fig.show()

for file in Path('business_tables/').glob('*.parquet'):
    figure_name = str(file).split('.parquet', 1)[0].split('/')[1]
    build_plot(file, figure_name)


