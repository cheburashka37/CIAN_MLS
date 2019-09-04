#!/usr/bin/env python
# coding: utf-8

# ## data preparation 

# In[1]:


import json
import ast

from pyspark.sql import Row
from pyspark.sql import SparkSession

from scipy.sparse import csr_matrix
#from scipy.sparse import dia_matrix
import numpy as np
import math

from scipy.sparse import find


# In[3]:


spark = SparkSession        .builder        .enableHiveSupport()        .getOrCreate()
#spark


# In[4]:


df_sopr = spark.read.table('prod.mles_sopr')
df_sopr.printSchema()


# In[5]:


rdd_sopr = df_sopr             .where("page_type = 'Card' or page_type = 'ListingFavorites'")             .select("user_id", "offer_id", "event_type", "page_type", "ptn_dadd")             .dropDuplicates()             .rdd


# In[112]:


import datetime
DICT_W_FOR_PAGE_TYPE = {"Card" : 3,
                        "CardJK" : 2,
                        "Listing" : 1,
                        "ListingFavorites" : 5}

DICT_W_FOR_EVENT_TYPE = {"card_show" : 3,
                        "phone_show" : 10}

#разделение на 9 частей по времени
def lambdaForArr_9(x):
    if x['ptn_dadd'] < datetime.date(2019, 6, 6):
        a = 0
    elif x['ptn_dadd'] < datetime.date(2019, 6, 11):
        a = 1
    elif x['ptn_dadd'] < datetime.date(2019, 6, 16):
        a = 2
    elif x['ptn_dadd'] < datetime.date(2019, 6, 21):
        a = 3
    elif x['ptn_dadd'] < datetime.date(2019, 6, 26):
        a = 4
    elif x['ptn_dadd'] < datetime.date(2019, 7, 1):
        a = 5
    elif x['ptn_dadd'] < datetime.date(2019, 7, 6):
        a = 6
    elif x['ptn_dadd'] < datetime.date(2019, 7, 11):
        a = 7
    elif x['ptn_dadd'] < datetime.date(2019, 7, 18):
        a = 8
    
    return (x['user_id'], [x['offer_id'], 
                           DICT_W_FOR_PAGE_TYPE[x['page_type']] * DICT_W_FOR_EVENT_TYPE[x['event_type']],
                           a])

#разделение на 3 части по времени
def lambdaForArr_3(x):
    if x['ptn_dadd'] < datetime.date(2019, 6, 16):
        a = 0
    elif x['ptn_dadd'] < datetime.date(2019, 7, 1):
        a = 1
    elif x['ptn_dadd'] < datetime.date(2019, 7, 18):
        a = 2

    
    return (x['user_id'], [x['offer_id'], 
                           DICT_W_FOR_PAGE_TYPE[x['page_type']] * DICT_W_FOR_EVENT_TYPE[x['event_type']],
                           a])


# In[7]:


arr_3 = rdd_sopr.map(lambda x: lambdaForArr_3(x))           .groupByKey()           .randomSplit([1, 500])[0].collect()


# In[8]:


#585887 users
len(arr_3)


# In[9]:


list(arr_3[0][1])


# In[10]:


len(arr_3)


# In[134]:


def get_mtrxs_9(arr):
    indptr = np.array([[0], [0], [0], [0], [0], [0], [0], [0], [0]])
    indices = [[], [], [], [], [], [], [], [], []]
    data = [[], [], [], [], [], [], [], [], []]

    
    for i in arr:
        if i[1]:
            ind = np.array([[0], [0], [0], [0], [0], [0], [0], [0], [0]])
            for j in i[1]:
                indices[j[2]].append(j[0])
                data[j[2]].append(j[1])
                ind[j[2]] += 1
            indptr = np.hstack((indptr, indptr[:, -1].reshape((9, 1)) + ind))
    
    #mtrx = csr_matrix((data, indices, indptr))
    mtrxs = []
    for i in range(9):
        mtrx = csr_matrix((data[i], indices[i], indptr[i]))
        mtrxs.append(mtrx)
    return mtrxs

def get_mtrxs_3(arr):
    indptr = np.array([[0], [0], [0]])
    indices = [[], [], []]
    data = [[], [], []]

    
    for i in arr:
        if i[1]:
            ind = np.array([[0], [0], [0]])
            for j in i[1]:
                indices[j[2]].append(j[0])
                data[j[2]].append(j[1])
                ind[j[2]] += 1
            indptr = np.hstack((indptr, indptr[:, -1].reshape((3, 1)) + ind))
    
    #mtrx = csr_matrix((data, indices, indptr))
    mtrxs = []
    for i in range(3):
        mtrx = csr_matrix((data[i], indices[i], indptr[i]))
        mtrxs.append(mtrx)
    return mtrxs


# In[12]:


mtrxs = get_mtrxs_3(arr_3)


# In[13]:


mtrxs


# In[55]:


#рекомендация UserDased для пользователя на косинусном расстоянии 
def recomCosUserBased(mtrx, user, k):
    recom = []
    U_u0 = distCosForUser(mtrx, user)
    ALPHACOS = 0.01
    a = find(U_u0 > ALPHACOS)[1]
    announOfUser = find(mtrx.getrow(user))[1]

    for r in find(mtrx[find((U_u0 > ALPHACOS))[1]])[1]:
        t = np.union1d(a, find(mtrx.getcol(r))[0]).size
        if (t != 0):
            b = np.intersect1d(a, find(mtrx.getcol(r))[0]).size/t
            if r not in announOfUser:
                recom.append((r, b))
                recom.sort(key=lambda x: x[1], reverse=True)
                recom = recom[:k]
    return recom


# In[56]:


#расстояния для вектора одного пользователя до векторов других пользователей, на евклидовой метрики
def distCosForUser(mtrx, user):
    userRow = mtrx.getrow(user)

    x = math.sqrt(int(userRow.dot(userRow.transpose())[0,0]))
    y = np.sqrt(mtrx.dot(mtrx.transpose()).diagonal())*x
    xy = userRow.dot(mtrx.transpose())

    return (xy.multiply(1/y)).toarray()[0]


# In[103]:


#рекомендация UserDased для пользователя на евклидовом расстоянии 
def recomEuclidUserBased(mtrx,user, k):
    recom = []
    ALPHAED = 50
    U_u0 = distEuclidForUser(mtrx, user)
    a = find(U_u0 < ALPHAED)[1]
    announOfUser = find(mtrx.getrow(user))[1]

    for r in find(mtrx[find((U_u0 < ALPHAED))[1]])[1]:
        b = np.intersect1d(a, find(mtrx.getcol(r))[0]).size/np.union1d(a, find(mtrx.getcol(r))[0]).size
        if r not in announOfUser:
            recom.append((r, b))
            recom.sort(key = lambda x: x[1], reverse=True)
            recom = recom[:k]
    return recom


# In[92]:


#расстояния для вектора одного пользователя до векторов других пользователей, на евклидовой метрики
def distEuclidForUser(mtrx, user):
    userRow = mtrx.getrow(user)
    
    x2 = np.ones(mtrx.shape[0]) * int(userRow.dot(userRow.transpose())[0,0])
    y2 = mtrx.dot(mtrx.transpose()).diagonal()
    _2xy = 2 * userRow.dot(mtrx.transpose())

    return np.array(np.sqrt(x2 + y2 - _2xy))[0]


# In[19]:


len(mtrxs)


# In[104]:


k = 5
sum = 0
for i in range(len(mtrxs) - 1):
    a = np.nonzero(mtrxs[i].getnnz(axis=1) > 0)[0]
    for j in a:
        recom = recomEuclidUserBased(mtrxs[i], j, k)
        
        t = len(find(mtrxs[i + 1].getrow(j))[1])
        if (t != 0):
            sum += len(np.intersect1d(recom, find(mtrxs[i + 1].getrow(j))[1]))/t
            #markUsers[j,i] = len(np.intersect1d(recom, find(mtrxs[i + 1].getrow(j))[1]))/t
            #print("    ", i, " j =", j, " sum =", sum)
            #print("                           t =", t, " recom -", recom)
        
    print(i)
sum = sum / mtrxs[0].shape[0]


# In[69]:


recom


# In[52]:


markUsers = np.zeros((mtrxs[2].shape[0],len(mtrxs) - 1))


# In[54]:


markUsers[432, 1]


# In[71]:


find(mtrxs[1])


# In[77]:


distEuclidForUser(mtrxs[1], 94)


# In[75]:


recom = []


# In[80]:


recom.append((0, 0))
recom.append((6, 2))
recom.append((1, 7))
recom.append((13, 9))


# In[82]:


recom[:7]


# In[85]:


recom.sort(key = lambda x: x[1])


# In[86]:


recom


# In[90]:


195245700 not in find(mtrxs[0].getrow(3))[1]


# In[137]:


arr_3 = rdd_sopr.map(lambda x: lambdaForArr_3_new(x))       .groupByKey()       .randomSplit([1, 1000])[0].collect()


# In[138]:


DICT_W_FOR_PAGE_TYPE#[list(arr_3[0][1])[0][2]]


# In[140]:


list(arr_3[0][1])


# In[ ]:


listSum = []
for weights in range(729):
    i_ = weights
    DICT_W_FOR_PAGE_TYPE[1] = i_ % 3 + 1
    i_ = i_ // 3
    DICT_W_FOR_PAGE_TYPE[2] = i_ % 3 + 1
    i_ = i_ // 3
    DICT_W_FOR_PAGE_TYPE[3] = i_ % 3 + 1
    i_ = i_ // 3
    DICT_W_FOR_PAGE_TYPE[4] = i_ % 3 + 1
    i_ = i_ // 3
    DICT_W_FOR_EVENT_TYPE[1] = i_ % 3 + 1
    i_ = i_ // 3
    DICT_W_FOR_EVENT_TYPE[2] = i_ % 3 + 1
    
    
    mtrxs = get_mtrxs_3_new(arr_3)
    
    k = 5
    sum = 0
    for i in range(len(mtrxs) - 1):
        a = np.nonzero(mtrxs[i].getnnz(axis=1) > 0)[0]
        for j in a:
            recom = recomCosUserBased(mtrxs[i], j, k)

            t = len(find(mtrxs[i + 1].getrow(j))[1])
            if (t != 0):
                sum += len(np.intersect1d(recom, find(mtrxs[i + 1].getrow(j))[1]))/t
                #markUsers[j,i] = len(np.intersect1d(recom, find(mtrxs[i + 1].getrow(j))[1]))/t
                #print("    ", i, " j =", j, " sum =", sum)
                #print("                           t =", t, " recom -", recom)

    print(weights)
    sum = sum / mtrxs[0].shape[0]
    
    listSum.append(sum)
    
#не забудь что listSum перевернут)! потому что криво работает % и //
# i_ = 123456
# DICT_W_FOR_PAGE_TYPE[1] = i_ % 10
# i_ = i_ // 10
# DICT_W_FOR_PAGE_TYPE[2] = i_ % 10
# i_ = i_ // 10
# DICT_W_FOR_PAGE_TYPE[3] = i_ % 10
# i_ = i_ // 10
# DICT_W_FOR_PAGE_TYPE[4] = i_ % 10
# i_ = i_ // 10
# DICT_W_FOR_EVENT_TYPE[1] = i_ % 10
# i_ = i_ // 10
# DICT_W_FOR_EVENT_TYPE[2] = i_ % 10


# DICT_W_FOR_PAGE_TYPE[2] выводит 5!!!

