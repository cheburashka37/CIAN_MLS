#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import ast

from pyspark.sql import Row
from pyspark.sql import SparkSession 

from scipy.sparse import csc_matrix
from scipy.sparse import dia_matrix
import numpy as np
import math

from scipy.sparse import find


# In[2]:


spark = SparkSession        .builder        .enableHiveSupport()        .getOrCreate()
#spark


# In[3]:


df_sopr = spark.read.table('prod.mles_sopr')
df_sopr.printSchema()


# In[4]:


arr = df_sopr             .where("page_type = 'Card' or page_type = 'ListingFavorites'")             .select("user_id", "offer_id")             .rdd             .map(lambda x: (x['user_id'], x['offer_id']))             .groupByKey()             .dropDuplicates()             .randomSplit([1.0, 100.0])[0]            .collect()
#rdd_sopr = rf_sopr_s.rdd
            


# In[5]:


#rdd_tuple_sopr = rdd_sopr.map(lambda x: (x['user_id'], x['offer_id']))
#rdd_tuple_sopr.take(1)


# In[6]:


#rdd_tuple_sopr_grouped = rdd_tuple_sopr.groupByKey()
#arr = rdd_tuple_sopr_grouped.collect()


# In[7]:


import numpy as np
from scipy.sparse import csr_matrix

def get_mtrx():
    indptr = [0]
    indices = []
    data = []

    for i in arr:
        if i[1]:
            for j in i[1]:
                indices.append(j)
            indptr.append(len(indices))
    
    data = np.ones(len(indices))
    #mtrx = csr_matrix((data, indices, indptr))
    return csr_matrix((data, indices, indptr))


# In[8]:


#import numpy as np
#data = np.ones(len(indices))


# In[9]:


#from scipy.sparse import csr_matrix

#mtrx = csr_matrix((data, indices, indptr))


# In[10]:


mtrx = get_mtrx()


# In[25]:


def distEuclid(user = None):
    test = (mtrx).dot(csr_matrix.transpose(mtrx))
    data = test.diagonal().reshape((1, test.shape[0])).repeat(2 * mtrx.shape[0] + 1, axis=0)
    offsets = np.arange(-mtrx.shape[0], mtrx.shape[0] + 1)
    test_dia = dia_matrix((data, offsets), shape = test.shape)
    
    data = None
    offsets = None
    test = (test_dia + test_dia.transpose() - 2 * test).sqrt()
    
    return test


# In[12]:


def distEuclidForUser(user):
    userRow = mtrx.getrow(user)
    
    x2 = np.ones(mtrx.shape[0]) * int(userRow.dot(userRow.transpose())[0,0])
    y2 = mtrx.dot(mtrx.transpose()).diagonal()
    _2xy = 2 * userRow.dot(mtrx.transpose())

    return np.array(np.sqrt(x2 + y2 - _2xy))[0]


# In[26]:


def distCosForUser(user):
    userRow = mtrx.getrow(user)

    x = math.sqrt(int(userRow.dot(userRow.transpose())[0,0]))
    y = np.sqrt(mtrx.dot(mtrx.transpose()).diagonal())*x
    xy = userRow.dot(mtrx.transpose())

    return (xy.multiply(1/y)).toarray()[0]


# In[14]:


def distPirson(user = None):
    rowForAvg = (mtrx.sum(axis=1) / mtrx.shape[0]).ravel().tolist()[0]
    #т.к. в rowForAvg значения порядка е-4 => distPirson = distCos
    return distCos(user)


# In[15]:


#distCos()


# In[16]:


OURUSER = 3126

#distance = distEuclid()


# In[19]:


#A.prune()


# In[20]:


def get_u_r(r):
    u_r = find(mtrx.getrow(r))[1]
    return u_r


# In[21]:


#find(mtrx[find((distance.getrow(OURUSER) < ALPHA))[1]])[1]#просмотры от (U(u0)) \in R


# In[22]:


#find((distance.getrow(OURUSER) < ALPHA))[1].size#U(u0)


# In[23]:


#r = 107547678
#find(mtrx.getcol(r))[0]#U(r)


# In[27]:


U_u0 = distCosForUser(OURUSER)


# In[49]:


def recomCos():
    max = 0
    recomend = 0
    ALPHACOS = 0.15
    a = find(U_u0 > ALPHACOS)[1]

    for r in find(mtrx[find((U_u0 > ALPHACOS))[1]])[1]:
        b = np.intersect1d(a, find(mtrx.getcol(r))[0]).size/np.union1d(a, find(mtrx.getcol(r))[0]).size
        if b > max:
            max = b
            recomend = r
            print((r, b))
    return (recomend, max)


# In[50]:


def recomEuclid():
    max = 0
    recomend = 0
    ALPHAED = 1.5
    a = find(U_u0 < ALPHAED)[1]

    for r in find(mtrx[find((U_u0 < ALPHAED))[1]])[1]:
        b = np.intersect1d(a, find(mtrx.getcol(r))[0]).size/np.union1d(a, find(mtrx.getcol(r))[0]).size
        if b > max:
            max = b
            recomend = r
            print((r, b))
    return (recomend, max)

