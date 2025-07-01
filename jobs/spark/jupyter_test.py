#!/usr/bin/env python
# coding: utf-8

# In[2]:




# In[4]:


import pyspark
pyspark.__version__


# In[11]:


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Simple S3A Test") \
    .getOrCreate()

try:
    df = spark.read.csv("s3a://bronze/olist_customers_dataset.csv", header=True)
    df.show()
    print("✅ 읽기 성공!")
except Exception as e:
    print(f"❌ 오류: {e}")
finally:
    spark.stop()


# In[12]:


df.printSchema()

