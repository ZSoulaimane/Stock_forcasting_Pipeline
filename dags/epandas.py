from numpy.core.fromnumeric import partition
from polygon import RESTClient
import datetime
import pandas as pd
import os

ROOT_PATH = os.getcwd()


def datastock (stock, d_from, d_to, nbr_time, type_time):

    key = "_8s1KxIVHNH2U69b86gCABD60AT7jINV"

    with RESTClient(key) as client:
        from_ = d_from
        to = d_to
        resp = client.stocks_equities_aggregates(stock, nbr_time, type_time, from_, to, unadjusted=False)

        data = resp.results
    
    #JSON TO DATAFRAME
    patients_df = pd.DataFrame(data)
    
    #OBJECT TO TIME
    patients_df['t'] = patients_df['t'].apply(lambda row: ts_to_datetime(row))
    
    #RENAME
    patients_df = patients_df.rename(columns = {'v': 'VOLUME', 'o': 'OPEN', 'c': 'CLOSE', 'h': 'HIGH', 'l': 'LOW', 't': 'DATE'})
    
    #ADD COLUMN
    patients_df['STOCK'] = stock
    
    #DROP UNECCESSARY COLUMNS
    patients_df= patients_df.drop(['vw', 'n'], axis=1)
    
    #ORGANIZE DATAFRAME
    patients_df = patients_df[["DATE", "STOCK", "OPEN", "CLOSE", "HIGH", "LOW", "VOLUME"]]
    
    #SAVE CSVs
    patients_df.to_csv(ROOT_PATH+'/airflow/data/'+type_time+'/'+stock+'.csv')
    #'~/data/'+stock+'.csv'
    #patients_df.to_csv('data/'+stock+'.csv')




def ts_to_datetime(ts) -> str:
    return (datetime.datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d'))



# daily 
def launchDay(start, end):
    liststock = ["AAPL", "AMZN"]
    for stock in liststock:
        datastock(stock, start, end, 1, "day")


# weekly
def launchWeek(start, end):
    liststock = ["AAPL", "AMZN"]
    for stock in liststock:
        datastock(stock, start, end, 1, "week")


# monthly 
def launchMonth(start, end):
    liststock = ["AAPL", "AMZN"]
    for stock in liststock:
        datastock(stock, start, end, 1, "month")


# yearly
def launchYear(start, end):
    liststock = ["AAPL", "AMZN"]
    for stock in liststock:
        datastock(stock, start, end, 1, "year")
