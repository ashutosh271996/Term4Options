#imports
import pandas as pd
from datetime import datetime
import numpy as np
import py_vollib.black_scholes_merton.implied_volatility
import py_vollib_vectorized

#reading
df = pd.read_csv("final_processed_data.csv",parse_dates=True)
df['TIMESTAMP']= pd.to_datetime(df['TIMESTAMP'])
df['EXPIRY_DT']= pd.to_datetime(df['EXPIRY_DT'])
df['DAYS_TO_EXP']=df['EXPIRY_DT']-df['TIMESTAMP']
df.loc[:,'OPTION_TYP'] = df['OPTION_TYP'].str.slice(0,1)
df.loc[:,'OPTION_TYP'] = df['OPTION_TYP'].str.lower()

#working on a subset of rows
df1=df[:15]

#Parameters
r = 0.05
q = 0

#IV
df1['IV'] = py_vollib_vectorized.vectorized_implied_volatility_black(df1['SETTLE_PR'],df1['CLOSE_y'],df1['STRIKE_PR'],df1['DAYS_TO_EXP']/np.timedelta64(1, 'D')/365,r,df1['OPTION_TYP'],q=q,model='black_scholes_merton')

#DELTA
df1['DELTA'] = py_vollib_vectorized.vectorized_delta(df1['OPTION_TYP'],df1['CLOSE_y'],df1['STRIKE_PR'],df1['DAYS_TO_EXP']/np.timedelta64(1, 'D')/365,r,df1['IV'],model='black_scholes')