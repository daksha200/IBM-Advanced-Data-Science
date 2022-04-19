#!/usr/bin/env python
# coding: utf-8

# # Assignment 3
# 
# Welcome to Assignment 3. This will be even more fun. Now we will calculate statistical measures. 
# 
# ## You only have to pass 4 out of 7 functions
# 
# Just make sure you hit the play button on each cell from top to down. There are seven functions you have to implement. Please also make sure than on each change on a function you hit the play button again on the corresponding cell to make it available to the rest of this notebook.

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

# In[5]:


from IPython.display import Markdown, display
def printmd(string):
    display(Markdown('# <span style="color:red">'+string+'</span>'))


if ('sc' in locals() or 'sc' in globals()):
    printmd('<<<<<!!!!! It seems that you are running in a IBM Watson Studio Apache Spark Notebook. Please run it in an IBM Watson Studio Default Runtime (without Apache Spark) !!!!!>>>>>')


# In[1]:


get_ipython().system('pip install pyspark==3.0')


# In[6]:


try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
except ImportError as e:
    printmd('<<<<<!!!!! Please restart your kernel after installing Apache Spark !!!!!>>>>>')


# In[7]:


sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

spark = SparkSession     .builder     .getOrCreate()


# All functions can be implemented using DataFrames, ApacheSparkSQL or RDDs. We are only interested in the result. You are given the reference to the data frame in the "df" parameter and in case you want to use SQL just use the "spark" parameter which is a reference to the global SparkSession object. Finally if you want to use RDDs just use "df.rdd" for obtaining a reference to the underlying RDD object. But we discurage using RDD at this point in time.
# 
# Let's start with the first function. Please calculate the minimal temperature for the test data set you have created. We've provided a little skeleton for you in case you want to use SQL. Everything can be implemented using SQL only if you like.

# In[28]:


def minTemperature():
    #TODO Please enter your code here, you are not required to use the template code below
    #some reference: https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame
    return spark.sql("SELECT MIN(temperature) as mintemp from washing").first().mintemp


# Please now do the same for the mean of the temperature

# In[29]:


def meanTemperature():
    #TODO Please enter your code here, you are not required to use the template code below
    #some reference: https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame
    return spark.sql("SELECT AVG(temperature) as meantemp from washing").first().meantemp


# Please now do the same for the maximum of the temperature

# In[30]:


def maxTemperature():
    #TODO Please enter your code here, you are not required to use the template code below
    #some reference: https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame
    return spark.sql("SELECT MAX(temperature) as maxtemp from washing").first().maxtemp


# Please now do the same for the standard deviation of the temperature

# In[11]:


def sdTemperature():
    #TODO Please enter your code here, you are not required to use the template code below
    #some reference: https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame
    #https://spark.apache.org/docs/2.3.0/api/sql/
    return spark.sql("SELECT STDDEV(temperature) as sdtemp from washing").first().sdtemp


# Please now do the same for the skew of the temperature. Since the SQL statement for this is a bit more complicated we've provided a skeleton for you. You have to insert custom code at four positions in order to make the function work. Alternatively you can also remove everything and implement if on your own. Note that we are making use of two previously defined functions, so please make sure they are correct. Also note that we are making use of python's string formatting capabilitis where the results of the two function calls to "meanTemperature" and "sdTemperature" are inserted at the "%s" symbols in the SQL string.

# In[32]:


def skewTemperature():    
    return spark.sql("""
SELECT 
    (
        count(temperature)/(count(temperature)-1)/(count(temperature)-2)
    ) *
    SUM (
        POWER(temperature-%s,3)/POWER(%s,3)
    )

as sktemperature from washing
                    """ %(meanTemperature(),sdTemperature())).first().sktemperature


# Kurtosis is the 4th statistical moment, so if you are smart you can make use of the code for skew which is the 3rd statistical moment. Actually only two things are different.

# In[33]:


def kurtosisTemperature():    
        return spark.sql("""
SELECT 
    (
        1/COUNT(temperature)
    ) *
    SUM (
        POWER(temperature-%s,4)/POWER(%s,4)
    )
as ktemperature from washing
                    """ %(meanTemperature(),sdTemperature())).first().ktemperature


# Just a hint. This can be solved easily using SQL as well, but as shown in the lecture also using RDDs.

# In[34]:


def correlationTemperatureHardness():
    #TODO Please enter your code here, you are not required to use the template code below
    #some reference: https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame
    #https://spark.apache.org/docs/2.3.0/api/sql/
    return spark.sql("SELECT corr(temperature,hardness) as temperaturehardness from washing").first().temperaturehardness


# Now it is time to grab a PARQUET file and create a dataframe out of it. Using SparkSQL you can handle it like a database. 

# In[35]:


get_ipython().system('wget https://github.com/IBM/coursera/blob/master/coursera_ds/washing.parquet?raw=true')
get_ipython().system('mv washing.parquet?raw=true washing.parquet')


# In[16]:


df = spark.read.parquet('washing.parquet')
df.createOrReplaceTempView('washing')
df.show()


# Now let's test the functions you've implemented

# In[36]:


min_temperature = 0
mean_temperature = 0
max_temperature = 0
sd_temperature = 0
skew_temperature = 0
kurtosis_temperature = 0
correlation_temperature = 0


# In[37]:


min_temperature = minTemperature()
print(min_temperature)


# In[38]:


mean_temperature = meanTemperature()
print(mean_temperature)


# In[39]:


max_temperature = maxTemperature()
print(max_temperature)


# In[40]:


sd_temperature = sdTemperature()
print(sd_temperature)


# In[41]:


skew_temperature = skewTemperature()
print(skew_temperature)


# In[42]:


kurtosis_temperature = kurtosisTemperature()
print(kurtosis_temperature)


# In[43]:


correlation_temperature = correlationTemperatureHardness()
print(correlation_temperature)


# Congratulations, you are done, please submit this notebook to the grader. 
# We have to install a little library in order to submit to coursera first.
# 
# Then, please provide your email address and obtain a submission token on the grader’s submission page in coursera, then execute the subsequent cells
# 
# ### Note: We've changed the grader in this assignment and will do so for the others soon since it gives less errors
# This means you can directly submit your solutions from this notebook

# In[44]:


get_ipython().system('rm -f rklib.py')
get_ipython().system('wget https://raw.githubusercontent.com/IBM/coursera/master/rklib.py')


# In[45]:


from rklib import submitAll
import json

key = "Suy4biHNEeimFQ479R3GjA"
email = "joshidaksha02@gmail.com"
token = "punfCh7EfPbvEMpU" #you can obtain it from the grader page on Coursera



# In[46]:


parts_data = {}
parts_data["FWMEL"] = json.dumps(min_temperature)
parts_data["3n3TK"] = json.dumps(mean_temperature)
parts_data["KD3By"] = json.dumps(max_temperature)
parts_data["06Zie"] = json.dumps(sd_temperature)
parts_data["Qc8bI"] = json.dumps(skew_temperature)
parts_data["LoqQi"] = json.dumps(kurtosis_temperature)
parts_data["ehNGV"] = json.dumps(correlation_temperature)



submitAll(email, token, key, parts_data)


# In[ ]:




