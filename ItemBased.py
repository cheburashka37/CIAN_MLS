#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import ast

from pyspark.sql import Row
from pyspark.sql import SparkSession 

import numpy as np
from scipy.sparse import csr_matrix


# In[2]:


spark = SparkSession        .builder        .enableHiveSupport()        .getOrCreate()
#spark


# In[3]:


df_sopr = spark.read.table('prod.mles_sopr')
df_sopr.printSchema()


# In[4]:


rdd_sopr = df_sopr             .where("page_type = 'Card' or page_type = 'ListingFavorites'")             .select("user_id", "offer_id")             .rdd
#rdd_sopr = rf_sopr_s.rdd


# In[5]:


#rdd_sopr.take(1)


# In[6]:


rdd_tuple_sopr = rdd_sopr.map(lambda x: (x['user_id'], x['offer_id']))
#rdd_tuple_sopr.take(1)


# In[7]:


rdd_tuple_sopr_grouped = rdd_tuple_sopr.groupByKey()                                         .dropDuplicates()                                         .randomSplit([1.0, 10.0])[0]     
arr = rdd_tuple_sopr_grouped.collect()


# In[8]:


indptr = [0]
indices = []
data = []

for i in arr:
    if i[1]:
        for j in i[1]:
            indices.append(j)
        indptr.append(len(indices))


# In[9]:


data = np.ones(len(indices))


# In[10]:


mtrx = csr_matrix((data, indices, indptr))


# In[11]:


OURUSER = 3
OURANNOUN = 206566560

def get_u_r(r):
    u_r = []
    for i in range(mtrx.shape[0]):
        if(OURUSER != i):
            if (int(mtrx[i, r]) == 1):
                u_r.append(i)
    return u_r


# In[12]:


u_r4ourannoun = get_u_r(OURANNOUN)


# In[14]:


indices_unic = list(set(indices))


# In[15]:


n_mtrx = mtrx[u_r4ourannoun]
avg_announ = csr_matrix((len(u_r4ourannoun), 1))

for i in indices_unic:
    avg_announ += n_mtrx[:, i]

avg_announ = avg_announ / len(indices_unic)


# In[55]:


def distEuclid(a,b):
    return float((csr_matrix.transpose(a - b).dot((a - b))).toarray())

def distCos(a,b):
    na = float((csr_matrix.transpose(a).dot(a)).toarray())
    nb = float((csr_matrix.transpose(b).dot(b)).toarray())
    if (na * nb == 0) :
        return 1001
    return float((csr_matrix.transpose(a).dot(b)).toarray()) /(na * nb)

def distPirson(a,b):
    return distCos(a - avg_announ, b - avg_announ)


# In[20]:


mtrx[u_r4ourannoun]


# In[64]:


max = 1000

for r in indices_unic:
    if (r != OURANNOUN):
        a = distPirson(n_mtrx[:, r], n_mtrx[:, OURANNOUN])
        if (abs(a) < max):
            print((r, abs(a)))
            recom = (r, abs(a))
            max = abs(a)


# In[65]:


recom

