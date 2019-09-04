#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import ast

from pyspark.sql import Row
from pyspark.sql import SparkSession


# In[2]:


from scipy.sparse import csr_matrix
from scipy.sparse import dia_matrix
import numpy as np
import math


# In[3]:


spark = SparkSession        .builder        .enableHiveSupport()        .getOrCreate()
#spark


# In[4]:


df_sopr = spark.read.table('prod.mles_sopr')
df_sopr.printSchema()


# In[5]:


rdd_sopr = df_sopr             .where("page_type = 'Card' or page_type = 'ListingFavorites'")             .select("user_id", "offer_id", "event_type", "page_type", "ptn_dadd")             .dropDuplicates()             .rdd


# In[6]:


import datetime

DICT_W_FOR_PAGE_TYPE = {"Card" : 3,
                        "CardJK" : 2,
                        "Listing" : 1,
                        "ListingFavorites" : 5}

DICT_W_FOR_EVENT_TYPE = {"card_show" : 3,
                        "phone_show" : 10}

def lambdaForArr(x):
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


# In[7]:


arr = rdd_sopr.map(lambda x: lambdaForArr(x))           .groupByKey()           .randomSplit([1.0, 10.0])[0]          .collect()


# In[8]:


list(arr[1][1])


# In[9]:


list(arr[0][1])


# In[ ]:





# In[10]:


import numpy as np
from scipy.sparse import csr_matrix

def get_mtrxs():
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


# In[11]:


mtrxs = get_mtrxs()


# In[12]:


mtrxs


# In[13]:


q = mtrxs[0].shape[0]


# In[14]:


from scipy.sparse import find


# In[15]:


def takeSecond(x):
    return x[1]


# In[20]:


def recomCosUserBased(mtrx, user, k):
    recom = [(0, 0) for i in range(k)]
    U_u0 = distCosForUser(mtrx, user)
    ALPHACOS = 0.01
    a = find(U_u0 > ALPHACOS)[1]

    for r in find(mtrx[find((U_u0 > ALPHACOS))[1]])[1]:
        t = np.union1d(a, find(mtrx.getcol(r))[0]).size
        if (t != 0):
            b = np.intersect1d(a, find(mtrx.getcol(r))[0]).size/t
            recom.append((r, b))
            recom.sort(key=lambda x: x[1], reverse=True)
            recom = recom[:k]
    return recom


# In[17]:


def distCosForUser(mtrx, user):
    userRow = mtrx.getrow(user)

    x = math.sqrt(int(userRow.dot(userRow.transpose())[0,0]))
    y = np.sqrt(mtrx.dot(mtrx.transpose()).diagonal())*x
    xy = userRow.dot(mtrx.transpose())

    return (xy.multiply(1/y)).toarray()[0]


# In[18]:


k = 5


# In[ ]:


sum = 0
for i in range(8):
    a = np.nonzero(mtrxs[i].getnnz(axis=1) > 0)[0]
    for j in a:
        recom = recomCosUserBased(mtrxs[i], j, k)
        
        sum += len(np.intersect1d(recom, find(mtrxs[i + 1].getrow(j))[1]))/k
        print("    ", i, " j = ", j, " sum = ", sum)
    print(i)
sum = sum / mtrxs[0].shape[0]

