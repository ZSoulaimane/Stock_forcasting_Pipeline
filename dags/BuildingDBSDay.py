import pandas as pd
import re
import mysql.connector
import os

ROOT_PATH = os.getcwd()

def clean_data(stocks_list):
    
    for i in range (len(stocks_list)):
    
        # data = pd.read_csv (r'C:/tmp/csvs/'+stocks_list[i]+'.csv')   data\AAPL.csv
        data = pd.read_csv(r''+ROOT_PATH+'/airflow/data/day/'+stocks_list[i]+'.csv')   
        df = pd.DataFrame(data)

        #REGEX TO TAKE ONLY DATE WITHOUT HOUR
        def extract_date(row):
            try:
                var = re.search(r'([0-9].*\s)', str(row['DATE'])).group(1)
                return var
            except:
                return row['Date']
            
        

        #APPLIYING DEVELOPED FUNCTION
        df['DATE'] = df.apply(lambda row: extract_date(row), axis=1)


        #FORCASTING DATE FORMAT INTO DATE ROW(OBJECT -> DATE)
        dates = pd.to_datetime(df["DATE"])
        df['DATE'] = pd.DatetimeIndex(dates)


        #UPDATE CSV INTO NEW CLEAN ONES
        df.to_csv('data/'+stocks_list[i]+'.csv')



def getConnection():
    # Connection to Postgre database
    connection = mysql.connector.connect(database="stockdb",
                            user='soule', password='soule', 
                            host='127.0.0.1', port='3306'
    )
    connection.autocommit = True
    return connection


#CREATE CURSOR
def getCursor(connection):
    return connection.cursor()
    

#CREATE TABLES
def createTables(cursor):
    # date day
    createTable41 = 'Create TABLE if not exists dateday(id_date int primary key AUTO_INCREMENT,date varchar(30));'
    cursor.execute(createTable41)
    
    # base daily 
    createTable4 = 'Create TABLE if not exists baseday(id_base int primary key AUTO_INCREMENT,id_stock integer, id_date integer, open float,close float,high float,low float,vloume float,CONSTRAINT fk_date2 FOREIGN KEY(id_date) REFERENCES dateday(id_date),CONSTRAINT fk_stock2 FOREIGN KEY(id_stock) REFERENCES stock(id_stock));'
    cursor.execute(createTable4)



#CREATE DATAFRAME
def dataframe(stock):
    data = pd.read_csv (r''+ROOT_PATH+'/airflow/data/day/'+stock+'.csv')   
    df = pd.DataFrame(data)
    return df


# INSERT DQTES INTO TABLE
def insert_date(connection, cursor,df):
    for row in df.itertuples():
        cursor.execute('''
                    INSERT INTO dateday (date)
                    VALUES (%s)
                    ''',
                    (row.DATE,)
                    )





# INSERT MESURES INTO FACT TABLE
def insertElements(connection, cursor, df, i):
    iddate = 1
    stocke = i + 1

    for row in df.itertuples():
        cursor.execute('''
                    INSERT INTO baseday (id_stock,id_date, open ,close ,high ,low ,vloume)
                    Values(%s,%s,%s,%s,%s,%s,%s)
                    ''',
                    (stocke,
                    iddate,
                    row.OPEN,
                    row.CLOSE,
                    row.HIGH,
                    row.LOW,
                    row.VOLUME)
            )

        iddate = iddate+1
        



def launchdbday(**context):
    

    #LIST OF STOCKs we will be using
    stocks_list = ["AAPL", "AMZN"]


    # CLEAN THE CSVs DATA
    #db.clean_data(stocks_list)
    
    #connection
    connection = getConnection()


    #cursor
    cursor = getCursor(connection)


    #creating tables
    createTables(cursor)


    #INSERTING STOCKS
    # insert_stock(connection, cursor, stocks_list)


    #GENERATING A DATAFRAME FOR INSERTING GENERATED DATES
    stock = dataframe("AAPL")


    # INSERT DATES
    insert_date(connection, cursor, stock)


    # INSERT STOCKS MESURES
    for i in range (len(stocks_list)):
        stock = dataframe(stocks_list[i])
        insertElements(connection, cursor, stock, i)



    connection.commit()
    connection.close() 
