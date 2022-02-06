import pandas as pd
import re
import mysql.connector
import os

ROOT_PATH = os.getcwd()

def clean_data(stocks_list):
    
    for i in range (len(stocks_list)):
    
        # data = pd.read_csv (r'C:/tmp/csvs/'+stocks_list[i]+'.csv')   data\AAPL.csv
        data = pd.read_csv(r''+ROOT_PATH+'/airflow/data/'+stocks_list[i]+'.csv')   
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
    
    #Stock 
    createTable1 = 'Create TABLE if not exists stock(id_stock int primary key AUTO_INCREMENT,stock varchar(30));'
    cursor.execute(createTable1)


    #Date
    
    createTable41 = 'Create TABLE if not exists dateday(id_date int primary key AUTO_INCREMENT,date varchar(30));'
    cursor.execute(createTable41)
    
    createTable42 = 'Create TABLE if not exists dateweek(id_date int primary key AUTO_INCREMENT,date varchar(30));'
    cursor.execute(createTable42)
    
    createTable43 = 'Create TABLE if not exists datemonth(id_date int primary key AUTO_INCREMENT,date varchar(30));'
    cursor.execute(createTable43)
    
    createTable44 = 'Create TABLE if not exists dateyear(id_date int primary key AUTO_INCREMENT,date varchar(30));'
    cursor.execute(createTable44)
    
    #Base

    createTable4 = 'Create TABLE if not exists baseday(id_base int primary key AUTO_INCREMENT,id_stock integer, id_date integer, open float,close float,high float,low float,vloume float,CONSTRAINT fk_date2 FOREIGN KEY(id_date) REFERENCES dateday(id_date),CONSTRAINT fk_stock2 FOREIGN KEY(id_stock) REFERENCES stock(id_stock));'
    cursor.execute(createTable4)

    createTable5 = 'Create TABLE if not exists baseweek(id_base int primary key AUTO_INCREMENT,id_stock integer, id_date integer, open float,close float,high float,low float,vloume float,CONSTRAINT fk_date3 FOREIGN KEY(id_date) REFERENCES dateweek(id_date),CONSTRAINT fk_stock3 FOREIGN KEY(id_stock) REFERENCES stock(id_stock));'
    cursor.execute(createTable5)

    createTable6 = 'Create TABLE if not exists basemonth(id_base int primary key AUTO_INCREMENT,id_stock integer, id_date integer, open float,close float,high float,low float,vloume float,CONSTRAINT fk_date4 FOREIGN KEY(id_date) REFERENCES datemonth(id_date),CONSTRAINT fk_stock4 FOREIGN KEY(id_stock) REFERENCES stock(id_stock));'
    cursor.execute(createTable6)

    createTable7 = 'Create TABLE if not exists baseyear(id_base int primary key AUTO_INCREMENT,id_stock integer, id_date integer, open float,close float,high float,low float,vloume float,CONSTRAINT fk_date5 FOREIGN KEY(id_date) REFERENCES dateyear(id_date),CONSTRAINT fk_stock5 FOREIGN KEY(id_stock) REFERENCES stock(id_stock));'
    cursor.execute(createTable7)




#CREATE DATAFRAME
def dataframe(stock):
    data = pd.read_csv (r''+ROOT_PATH+'/airflow/data/day/'+stock+'.csv')   
    df = pd.DataFrame(data)
    return df



# INSERT STOCKS INTO TABLE
def insert_stock(connection, cursor, stocks_list):
    for stock in stocks_list:
        cursor.execute('''
                        INSERT INTO stock (stock)
                        Values(%s)
                        ''',
                        (stock,)
                        )


def launchdb(**context):
    

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
    insert_stock(connection, cursor, stocks_list)


    #GENERATING A DATAFRAME FOR INSERTING GENERATED DATES
    # stock = dataframe("AAPL")


    # INSERT DATES
    # insert_date(connection, cursor, stock)


    # INSERT STOCKS MESURES
    # for i in range (len(stocks_list)):
    #     stock = dataframe(stocks_list[i])
    #     insertElements(connection, cursor, stock, i)



    connection.commit()
    connection.close() 
