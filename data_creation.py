#imports
from datetime import datetime
from dateutil.relativedelta import relativedelta
import  zipfile, os, io
import requests
import pandas as pd
import sys

#parameters
start_date = "2019-01-01"
last_date = "2020-07-10"
base = ''

#downloading data
dmonth = {'01':'JAN','02':'FEB','03':'MAR','04':'APR','05':'MAY','06':'JUN',
          '07':'JUL','08':'AUG','09':'SEP','10':'OCT','11':'NOV','12':'DEC'}


firstdt = datetime.strptime(start_date,'%Y-%m-%d')

lastdt = datetime.strptime(last_date,'%Y-%m-%d')

diff, wr = lastdt.date()-firstdt.date(), ''


for i in range(1,diff.days+1):
    nextdt = firstdt+ relativedelta(days=i)
    d, m, y = '%02d' % nextdt.day, '%02d' % nextdt.month, '%02d' % nextdt.year
    if not os.path.isdir(base+"dataset"):
        os.mkdir(base+"dataset")
        os.mkdir(base+"dataset"+'/Stocks')
        os.mkdir(base+"dataset"+'/Futures')
    zpath = base+"dataset"+'/'+d+'.zip'
    
    
    while True:
        try:
            url = 'https://www1.nseindia.com/content/historical/EQUITIES/'+y+'/'+dmonth[m]+'/cm'+d+dmonth[m]+y+'bhav.csv.zip'
            a=requests.get(url)
        except requests.ConnectionError:
            print('No connection, retrying')
        break
            
    if a.status_code==200:
        dload=open(zpath, 'wb')
        dload.write(a.content)
        dload.close()
        
        z = zipfile.ZipFile(zpath, 'r')
        z.extractall(base+"dataset/Stocks"+'/')
        z.close()
        os.remove(zpath)
        
        f, deldict = pd.read_csv(base+"dataset/Stocks"+'/cm'+d+dmonth[m]+y+'bhav.csv'), {}  #reading the raw dl-ed bhav file
        f = f[f['SERIES'] == 'EQ'] #retaining only EQ rows and leaving out bonds,options etc
        deliverable = requests.get('https://www1.nseindia.com/archives/equities/mto/MTO_'+d+m+y+'.DAT').text.splitlines()
        
        del deliverable[:4]

        for i in deliverable:
            c = i.split(',')
            if c[3] == 'EQ' :                
                deldict[c[2]] = c[5] #building delivarables dict
     
        dfdel = pd.DataFrame(list(deldict.items()), columns = ['SYMBOL', 'DELIVERABLE'])
        f = f.merge(dfdel, on='SYMBOL', how='left')      #left merge of delivarables here
        
        # indices = requests.get('https://www1.nseindia.com/content/indices/ind_close_all_'+d+m+y+'.csv').content

        # #sometimes nse doesnt give the index file, so the if condition
        # if len(indices)>300:
        #     indx = pd.read_csv(io.StringIO(indices.decode('utf-8'))) #reading content of indices csv and storing in DataFrame using io module
        #     indx.to_csv(base+"dataset"+'/Index/Indices'+ str(nextdt.date())+'.csv', index=False)
        #     indx[['Index Name', 'Index Date', 'Open Index Value', 'High Index Value', 'Low Index Value', 'Closing Index Value', 'Volume']]
            
        #     indx = indx.rename(columns={'Index Name' : 'SYMBOL', 'Index Date' : 'TIMESTAMP', 'Open Index Value' : 'OPEN', 
        #                                 'High Index Value' : 'HIGH', 'Low Index Value' : 'LOW', 'Closing Index Value' : 'CLOSE', 
        #                                 'Volume' : 'TOTTRDQTY'})
            
        #     f=f.append(indx, ignore_index=True)
        
        f['TIMESTAMP'] = pd.Series(str(nextdt.date().strftime('%Y%m%d')) for _ in range(len(f)))
        f = f[['SYMBOL', 'TIMESTAMP', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'TOTTRDQTY', 'DELIVERABLE']]
        f.to_csv(base+"dataset"+'/Stocks/'+str(nextdt.date())+'.csv', index=False)
        os.remove(base+"dataset/Stocks"+'/cm'+d+dmonth[m]+y+'bhav.csv')
        
        futures = requests.get('https://www1.nseindia.com/content/historical/DERIVATIVES/'+y+'/'+dmonth[m]+'/fo'+d+dmonth[m]+y+'bhav.csv.zip')
        fo = open(zpath, 'wb')
        fo.write(futures.content)
        fo.close()
        
        z, wr = zipfile.ZipFile(zpath,'r'), nextdt.date()
        z.extractall(base+"dataset"+'/Futures')
        z.close()
        os.remove(zpath)
        
#merging files
!tail -q -n+2 dataset/Stocks/*.csv > dataset/all_stocks.csv
!tail -q -n+2 dataset/Futures/*.csv > dataset/all_futures.csv

#pre-processing
stocks_columns = pd.read_csv('dataset/Stocks/2019-01-02.csv').columns
futures_columns = pd.read_csv('dataset/Futures/fo02JAN2019bhav.csv', usecols=[*range(0, 15)]).columns
stocks_data= pd.read_csv('dataset/all_stocks.csv', header=None)
futures_data= pd.read_csv('dataset/all_futures.csv', header=None, usecols=[*range(0, 15)])
stocks_data.columns = stocks_columns
futures_data.columns = futures_columns

stocks_data['TIMESTAMP']= pd.to_datetime(stocks_data['TIMESTAMP'], format='%Y%m%d')
futures_data['TIMESTAMP']= pd.to_datetime(futures_data['TIMESTAMP'])
futures_data['EXPIRY_DT']= pd.to_datetime(futures_data['EXPIRY_DT'])

futures_data=futures_data.loc[futures_data['INSTRUMENT'] == "OPTSTK"]
result_data = pd.merge(futures_data, stocks_data, how='left', on=['SYMBOL', 'TIMESTAMP'])
result_data.to_csv('final_processed_data.csv',index=False)