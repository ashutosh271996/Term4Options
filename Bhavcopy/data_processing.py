from datetime import datetime
from dateutil.relativedelta import relativedelta
import  zipfile, os, io
import requests
import pandas as pd
import sys
import datetime
from dateutil.parser import parse
import mibian
import scipy

def compute_DAYS_TO_EXP (row):
   return (parse(row["EXPIRY_DT"])-row["TIMESTAMP"]).days

def compute_IV (row):  
   if row['OPTION_TYP'] == "CE" :
      return mibian.BS([float(row["STOCK_CLOSE"]), float(row["STRIKE_PR"]),r, float(row["DAYS_TO_EXP"]) ], callPrice=float(row["CLOSE"]) ).impliedVolatility
   return  mibian.BS([float(row["STOCK_CLOSE"]), float(row["STRIKE_PR"]),r, float(row["DAYS_TO_EXP"]) ], putPrice=float(row["CLOSE"]) ).impliedVolatility

def compute_DELTA (row):  
   return  mibian.BS([float(row["STOCK_CLOSE"]), float(row["STRIKE_PR"]),r, float(row["DAYS_TO_EXP"]) ], volatility=row["IV"] ).callDelta

def compute_MONEY (row):  
   return  float(row["STRIKE_PR"])/float(row["STOCK_CLOSE"])


stocks_data= pd.read_csv ('/Users/ashutoshaggawal27/Documents/Term4/ProjectCourse/Term4Options/dataset/stocks.csv')
futures_data= pd.read_csv ('/Users/ashutoshaggawal27/Documents/Term4/ProjectCourse/Term4Options/dataset/futures.csv')
stocks_data=stocks_data.loc[stocks_data['SYMBOL'] != "SYMBOL"]
futures_data=futures_data.loc[futures_data['SYMBOL'] != "SYMBOL"]


stocks_data.rename(columns={'OPEN': 'STOCK_OPEN', 'HIGH': 'STOCK_HIGH','LOW': 'STOCK_LOW','CLOSE': 'STOCK_CLOSE','TOTTRDQTY': 'STOCK_TOTTRDQTY','DELIVERABLE': 'STOCK_DELIVERABLE'}, inplace=True)
futures_data.drop(futures_data.columns[futures_data.columns.str.contains('unnamed',case = False)],axis = 1, inplace = True)
futures_data=futures_data.loc[futures_data['INSTRUMENT'] == "OPTSTK"]
futures_data.reset_index(drop=True, inplace=True)
stocks_data = stocks_data.astype(str)
futures_data = futures_data.astype(str)

futures_data.TIMESTAMP=futures_data.TIMESTAMP.apply(lambda x : parse(x))
stocks_data.TIMESTAMP=stocks_data.TIMESTAMP.apply(lambda x : parse(x))

# print(len(stocks_data))
# print(len(futures_data))
# print(stocks_data)
# print(futures_data)
# print(stocks_data.dtypes)
# print(futures_data.dtypes)

result_data = pd.merge(futures_data, stocks_data, how='left', on=['SYMBOL', 'TIMESTAMP'])
#print(result_data)

r=sys.argv[1]



result_data['DAYS_TO_EXP']=result_data.apply(compute_DAYS_TO_EXP, axis=1)

#print(result_data)

result_data['IV'] = result_data.apply(compute_IV, axis=1)
#print(result_data)

result_data['DELTA'] = result_data.apply(compute_DELTA, axis=1)
#print(result_data)

result_data['MONEY'] = result_data.apply(compute_MONEY, axis=1)
#print(result_data)


result_data.to_csv('/Users/ashutoshaggawal27/Documents/Term4/ProjectCourse/Term4Options/dataset/options_with_greeks.csv', encoding='utf-8', index=False)
