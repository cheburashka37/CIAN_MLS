
import json
import ast

from pyspark.sql import Row
from pyspark.sql import SparkSession 

def cur_dict(**x):
    if len(x) < 14:
        r_key = ['lng', 'description', 'floorsCount', 'price', 'dateInserted', 'roomsCount', 
                 'priceType', 'status', 'address', 'category', 'floorNumber', 'announcementid', 
                 'lat', 'totalArea']
        key = list(x.keys())
        for i in r_key:
            if not (i in key) :
                x[i] = 'Null'
    x['dadd'] = x['dateInserted'][:10]
    return Row(**x)

spark = SparkSession \
    .builder \
    .enableHiveSupport() \
    .getOrCreate()
#spark

df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.156.0.3:6667,10.156.0.4:6667,10.156.0.5:6667,") \
    .option("subscribe", "mles.announcements") \
    .load() 

df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
#df.printSchema()

rdd = df.select("value").rdd
rdd_json = rdd.map(lambda x: ast.literal_eval(x.asDict()['value']))

rdd_row = rdd_json.map(lambda x: cur_dict(**x))

df2 = spark.createDataFrame(rdd_row)
#df2.collect()

df2.write.format('orc').mode('overwrite').partitionBy('dadd').saveAsTable('sschokorov.announcement_parsed')
