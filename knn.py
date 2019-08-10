#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import ast

from pyspark.sql import Row
from pyspark.sql import SparkSession 


# In[2]:


spark = SparkSession        .builder        .enableHiveSupport()        .getOrCreate()
#spark

USER_ID_FIND = '331339c3-9cb0-41bc-b37c-bcecc8252997'


# In[3]:


df_sopr = spark.read.table('prod.mles_sopr')
df_sopr.printSchema()


# In[4]:


df_lst = spark.read.table('prod.announcement_lst')
df_lst.printSchema()


# In[5]:


rdd_sopr = df_sopr.select("user_id", "offer_id").dropDuplicates().rdd#.show()
#rdd_lst = df_lst.limit(10000).rdd#.show()


# In[6]:


from pyspark.ml.feature import HashingTF, IDF, Tokenizer

df_tfidf_lst = df_lst.randomSplit([1.0, 25.0])[0]

tokenizer = Tokenizer(inputCol="description", outputCol="words")
wordsData = tokenizer.transform(df_tfidf_lst)

hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
featurizedData = hashingTF.transform(wordsData)
# alternatively, CountVectorizer can also be used to get term frequency vectors

idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

rdd_lst = rescaledData.select("announcementid", "features", "lat", "lng", "totalArea", "floorNumber", 
                    "floorsCount", "category", "roomsCount", "totalArea", "price", "announcementid").rdd#.show()


# In[8]:


#find proposals for user_id = 000e7f64-ecf2-4658-a9c0-e2b970f87ad7
rdd_user_sopr = rdd_sopr.filter(lambda x: x['user_id'] == USER_ID_FIND)
rdd_user_sopr_c = rdd_user_sopr.map(lambda x: x[1])
ListOfAnnounId4User = rdd_user_sopr_c.collect() #лист из id объявелний которые просматривал пользователь


# In[9]:


ListOfAnnounId4User


# In[10]:


rdd_user_lst = rdd_lst.filter(lambda x: x['announcementid'] in ListOfAnnounId4User)
rdd_user_lst_c = rdd_user_lst.map(lambda x: x.asDict())
FeaturesOfAnnoun4User = rdd_user_lst_c.collect() #лист из самих объявелний которые просматривал пользователь


# In[11]:


FeaturesOfAnnoun4User


# In[12]:


import math
from numpy import array
DICT = {'bedRent': 1,
        'commercialLandRent': 2,
        'dailyHouseRent': 3,
        'flatRent': 4,
        'flatSale': 5,
        'freeAppointmentObjectRent': 6,
        'officeRent': 7,
        'roomRent': 8,
        'shoppingAreaRent': 9,
        'warehouseSale': 10,
        'cottageSale': 11,
        'flatShareSale': 12,
        'freeAppointmentObjectSale': 13,
        'garageRent': 14,
        'garageSale': 15,
        'industryRent': 16,
        'newBuildingFlatSale': 17,
        'townhouseRent': 18,
        'buildingSale': 19,
        'cottageRent': 20,
        'dailyFlatRent': 21,
        'houseRent': 22,
        'houseSale': 23,
        'landSale': 24,
        'roomSale': 25,
        'warehouseRent': 26,
        'buildingRent': 27,
        'shoppingAreaSale': 28,
        'townhouseSale': 29,
        'businessRent': 30,
        'businessSale': 31,
        'dailyBedRent': 32,
        'dailyRoomRent': 33,
        'houseShareRent': 34,
        'houseShareSale': 35,
        'industrySale': 36,
        'officeSale': 37,
        'commercialLandSale': 38}
def lambda_for_rdd_lst(x): 
    #функция для lst, сразу считает расстояние по сумме квадратов для каждого объекта до 
    #векторов объявлений просмотренными пользователем
    sum = 0
    for i in FeaturesOfAnnoun4User:
        sum+= x['features'].squared_distance(i['features'])
        
        if(x['lat'] is not None and i['lat'] is not None):
            sum += pow(x['lat'] - i['lat'], 2)
        elif (x['lat'] is not None):
            sum += pow(x['lat']*0.25, 2)
        elif (i['lat'] is not None):
            sum += pow(i['lat']*0.25, 2)
        
        if(x['lng'] is not None and i['lng'] is not None): 
            sum += pow( x['lng'] - i['lng'], 2)
        elif (x['lng'] is not None): 
            sum += pow( x['lng']* 0.25, 2)
        elif (i['lng'] is not None): 
            sum += pow( i['lng']* 0.25, 2)
        
        if(x['floorNumber'] is not None and i['floorNumber'] is not None): 
            sum += pow( x['floorNumber'] - i['floorNumber'], 2)
        elif (x['floorNumber'] is not None): 
            sum += pow( x['floorNumber']* 0.25, 2)
        elif (i['floorNumber'] is not None): 
            sum += pow( i['floorNumber']* 0.25, 2)
        
        if(x['floorsCount'] is not None and i['floorsCount'] is not None): 
            sum += pow( x['floorsCount'] - i['floorsCount'], 2)
        elif (x['floorsCount'] is not None): 
            sum += pow( x['floorsCount']* 0.25, 2)
        elif (i['floorsCount'] is not None): 
            sum += pow( i['floorsCount']* 0.25, 2)
        
        if(x['category'] is not None and i['category'] is not None): 
            sum += pow( 10000 * (DICT[x['category']] - DICT[i['category']]), 2)
        elif (x['category'] is not None): 
            sum += pow( DICT[x['category']] * 2500, 2)
        elif (i['category'] is not None): 
            sum += pow( DICT[i['category']] * 2500, 2)
        
        if(x['roomsCount'] is not None and i['roomsCount'] is not None): 
            sum += pow( x['roomsCount'] - i['roomsCount'], 2)
        elif (x['roomsCount'] is not None): 
            sum += pow( x['roomsCount']* 0.25, 2)
        elif (i['roomsCount'] is not None): 
            sum += pow( i['roomsCount']* 0.25, 2)
        
        if(x['totalArea'] is not None and i['totalArea'] is not None): 
            sum += pow( x['totalArea'] - i['totalArea'], 2)
        elif (x['totalArea'] is not None): 
            sum += pow( x['totalArea']* 0.25, 2)
        elif (i['totalArea'] is not None): 
            sum += pow( i['totalArea']* 0.25, 2)
        
        if(x['price'] is not None and i['price'] is not None): 
            sum += pow( math.log(x['price']) - math.log(i['price']), 2)
        elif (x['price'] is not None): 
            sum += pow( math.log(x['price'])* 0.25, 2)
        elif (i['price'] is not None): 
            sum += pow( math.log(i['price'])* 0.25, 2)

    return (x['announcementid'], sum)


# In[13]:


#lambda_for_rdd_lst(rdd_lst.take(1)[0].asDict())


# In[15]:


#from pyspark.mllib.linalg.distributed import CoordinateMatrix, MatrixEntry
rdd_parsed_lst = rdd_lst.map(lambda x: lambda_for_rdd_lst(x))
#list_rdd_parsed_lst = rdd_parsed_lst.collect()


# In[16]:


#len(list_rdd_parsed_lst)


# In[17]:


def compare(x, y):
    #функция сравнения расстояний для объявлений. Объявления, 
    #которые пользователь уже просматривал, не сравниваются!
    if x[0] in ListOfAnnounId4User:
        return y
    if y[0] in ListOfAnnounId4User:
        return x
    if x[1] > y[1]:
        return y
    if x[1] <= y[1]:
        return x


# In[18]:


ListOfAnnounId4User


# In[19]:


nearest = rdd_parsed_lst.reduce(lambda a, b: compare(a, b))


# In[20]:


#ближайший вектор объявления, с его расстоянием до просмотренных пользователем.
nearest

