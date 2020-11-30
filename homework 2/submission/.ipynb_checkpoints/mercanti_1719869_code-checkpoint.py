import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def add_readble_time(start_date, df):
    df = df.copy()
    df.insert(0, "date", [start_date + timedelta(seconds=i) for i in df["time"]])
    return df

def compute_R(data_open, k):  
    # computes the rolling window's sum backwards
    m_pos = data_open.rolling(window = k).sum()
    
    # computes the rolling window's sum forwards 
    m_neg = data_open.iloc[::-1].rolling(window = k).sum().iloc[::-1]   
    return (m_pos - m_neg)/m_neg.fillna(1)

def make_ohlc(price, size, resample_time, k):  
    # computes price in USD and creates ohlc
    price_data = price.apply(lambda x: x/10000)
    price_data = price_data.resample(resample_time).ohlc()
    price_data = price_data.fillna(method = 'ffill')
    
    # volumes
    vol_data = size.resample(resample_time).sum()
    vol_data = pd.DataFrame(vol_data.rename('volume').fillna(method = 'ffill'))
    
    # add R values
    df = pd.concat([price_data, vol_data], axis = 1)
    df['R'] = compute_R(df['open'], k)  
    return df


# load data
order_cols =[]
for i in range(1, 11):
     order_cols.extend(["sell_" + str(i), "vsell_" + str(i), "buy_" + str(i), "vbuy_" + str(i)])
        
col_names = {"orderbook": order_cols,
            "message": ["time", "event_type", "order_id", "size", "price", "direction"]}

messages = pd.read_csv("data/AAPL_2012-06-21_34200000_57600000_message_10.csv", names=col_names["message"])
orderbook = pd.read_csv("data/AAPL_2012-06-21_34200000_57600000_orderbook_10.csv", names=col_names["orderbook"])

# merge orderbook and messages
data = messages.copy()
data[col_names["orderbook"]] = orderbook

# set correct date format
start_date = datetime.strptime("21.06.2012", "%d.%m.%Y")
data = add_readble_time(start_date, data)

# consider only executed orders
executed = data[(data["event_type"].isin([4,5]))]
executed.index = executed["date"]

# k to compute the R value
k=5
times = ['1s', '10s', '1Min', '10Min'] 

# list with different ohlcv
out = [ make_ohlc(price = executed['price'], 
                  size = executed['size'], 
                  resample_time = time, 
                  k = k) for time in times]

# save output
for el, time  in zip(out, times):
    el.to_csv('mercanti_1719869_output_' + time + '_csv.csv')

