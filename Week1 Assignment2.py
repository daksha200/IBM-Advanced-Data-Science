#!/usr/bin/env python
# coding: utf-8

# # Warmup Assignment 2
# Below you see some ApacheSpark code written in Python. You don't have to change code now, the only thing we want you to do is to make sure that you have a proper Apache Spark Notebook environment available for this course
# 
# 

# This notebook is designed to run in a IBM Watson Studio default runtime (NOT the Watson Studio Apache Spark Runtime as the default runtime with 1 vCPU is free of charge). Therefore, we install Apache Spark in local mode for test purposes only. Please don't use it in production.
# 
# In case you are facing issues, please read the following two documents first:
# 
# https://github.com/IBM/skillsnetwork/wiki/Environment-Setup
# 
# https://github.com/IBM/skillsnetwork/wiki/FAQ
# 
# Then, please feel free to ask:
# 
# https://coursera.org/learn/machine-learning-big-data-apache-spark/discussions/all
# 
# Please make sure to follow the guidelines before asking a question:
# 
# https://github.com/IBM/skillsnetwork/wiki/FAQ#im-feeling-lost-and-confused-please-help-me
# 
# 
# If running outside Watson Studio, this should work as well. In case you are running in an Apache Spark context outside Watson Studio, please remove the Apache Spark setup in the first notebook cells.

# In[1]:


from IPython.display import Markdown, display
def printmd(string):
    display(Markdown('# <span style="color:red">'+string+'</span>'))


if ('sc' in locals() or 'sc' in globals()):
    printmd('<<<<<!!!!! It seems that you are running in a IBM Watson Studio Apache Spark Notebook. Please run it in an IBM Watson Studio Default Runtime (without Apache Spark) !!!!!>>>>>')


# In[2]:


get_ipython().system('pip install pyspark==3.0')


# In[3]:


try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
except ImportError as e:
    printmd('<<<<<!!!!! Please restart your kernel after installing Apache Spark !!!!!>>>>>')


# In[4]:


sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

spark = SparkSession     .builder     .getOrCreate()


# In[5]:


def assignment1(sc):
    rdd = sc.parallelize(list(range(100)))
    return rdd.count()


# In[6]:


print(assignment1(sc))


# In[7]:


get_ipython().system('rm -f rklib.py')
get_ipython().system('wget https://raw.githubusercontent.com/IBM/coursera/master/rklib.py')


# Please provide your email address and obtain a submission token on the grader’s submission page in coursera, then execute the cell

# In[9]:


from rklib import submit
import json

key = "R1eDmiHNEei9kxIYdin0mA"
part = "fnFg7"
email = "joshidaksha02@gmail.com"
token = "h6LmnThhEGNATEdL" #you can obtain it from the grader page on Coursera (have a look here if you need more information https://youtu.be/GcDo0Rwe06U?t=276)


submit(email, token, key, part, [part], json.dumps(assignment1(sc)))


# In[ ]:




