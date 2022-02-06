from polygon import RESTClient
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,col
from pyspark.sql.types import StringType
from pyspark.sql.functions import lit

class Extraction(object):

    def __init__(self):
        super().__init__()
        self.spark = SparkSession.builder.getOrCreate()



    def datastock (self,stock, d_from, d_to, nbr_time, type_time):
    
     self.spark = SparkSession.builder \
     .getOrCreate()


     key = "_8s1KxIVHNH2U69b86gCABD60AT7jINV"

     with RESTClient(key) as client:
          from_ = d_from
          to = d_to
          resp = client.stocks_equities_aggregates(stock, nbr_time, type_time, from_, to, unadjusted=False)

          data = resp.results
        
          # print(data)

          # for result in resp.results:
          #      dt = ts_to_datetime(result["t"])
          #      print(f"{dt}\n\tO: {result['o']}\n\tH: {result['h']}\n\tL: {result['l']}\n\tC: {result['c']} ")



          rdd = self.spark.sparkContext.parallelize(data)
          myDf = self.spark.read.json(rdd)
          udf_star_desc = udf(lambda x:ts_to_datetime(x),StringType())
          data = myDf.withColumn("d",udf_star_desc(col("t")))\
              .withColumn("s",lit(stock).cast(StringType()))\
        
        

          data1 = data.select( col("d").alias("Date")\
             ,col("s").alias("Stock"),\
                  col("o").alias("OPEN"),\
                   col("c").alias("CLOSE"),\
                            col("h").alias("HIGH"),\
                                 col("l").alias("LOW"),\
                                      col("v").alias("VOLUME"))
        
          #data1.repartition(1).write.format("csv").save("sv")


          dataframe = data1.toPandas()
     dataframe.to_csv(stock+".csv")

def ts_to_datetime(ts) -> str:
    return datetime.datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M')

Ext = Extraction()
liststock = ["AAPL", "AMZN", "TSLA", "FB", "PYPL"]
for stock in liststock:
    Ext.datastock(stock,"2019-01-01", "2021-02-01", 1, "day")