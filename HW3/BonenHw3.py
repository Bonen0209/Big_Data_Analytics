
# coding: utf-8

# In[1]:


#Initial the spark
import os
import sys

#this part is used for pyspark submit
os.environ['PYSPARK_SUBMIT_ARGS']='--verbose --master=yarn --deploy-mode=client pyspark-shell'

os.environ['JAVA_HOME']='/usr/lib/jvm/java-8-openjdk-amd64/'
os.environ['YARN_CONF_DIR']='/etc/alternatives/hadoop-conf/'

#this line is used for spark2.2
os.environ['SPARK_HOME']='/opt/cloudera/parcels/SPARK2-2.2.0.cloudera2-1.cdh5.12.0.p0.232957/lib/spark2'

#this line is used for python3.5
os.environ['PYSPARK_PYTHON']='/usr/bin/python3'

spark_home = os.environ.get('SPARK_HOME', None)
sys.path.insert(0, os.path.join(spark_home, 'python'))
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.10.4-src.zip'))  
exec(open(os.path.join(spark_home, 'python/pyspark/shell.py')).read())


# In[2]:


#Import to change the types
from pyspark.sql.types import *

#Read the file
spWaferlog = spark.read              .format('csv')              .option('header', 'true')              .load('/user/homework_2/WaferLog.csv')

spYield = spark.read           .format('csv')           .option('header', 'true')           .load('/user/homework_1/Yield.csv')

#Rename the title
spYield = spYield.withColumnRenamed('_c0', 'Wafer_ID')

oldColumns = ['_c0', 'Lot.ID', 'Wafer.ID', 'Process.stage', 'Tool.ID']
newColumns = ['number', 'Lot_ID', 'Wafer_ID', 'Process_stage', 'Tool_ID']

for i in range(len(oldColumns)):
    spWaferlog = spWaferlog.withColumnRenamed(oldColumns[i], newColumns[i])
            
#Change the schema
spYield = spYield.withColumn('yield', spYield['yield'].cast(DoubleType()))
spWaferlog = spWaferlog.withColumn('Process_stage', spWaferlog['Process_stage'].cast(IntegerType()))

#Join the data
spData = spYield.join(spWaferlog, 'Wafer_ID')

#Pick the useful information
spData = spData.select('Wafer_ID', 'yield', 'Process_stage', 'Time', 'Action')                .orderBy('Wafer_ID', 'Process_stage', 'Action')


# In[3]:


#Seperate the LOGIN and LOGOUT time
spIn = spData.where("Action == 'LOGIN_TOOL'")
spOut = spData.where("Action == 'LOGOUT_TOOL'")

#Rename the title
spIn = spIn.withColumnRenamed('Time', 'Qtime_OUT')
spOut = spOut.withColumnRenamed('Time', 'Qtime_IN')

#Minus the Process_stage
spIn = spIn.withColumn('Process_stage', spIn['Process_stage'] - 1)            .select('Wafer_ID', 'Process_stage', 'Qtime_OUT')            .where("Process_stage > 0")

#Join two tables
joinTables = ['Wafer_ID', 'Process_stage']
spTotal = spOut.join(spIn, joinTables).orderBy('Wafer_ID', 'Process_stage')

#Drop the Action
spTotal = spTotal.drop('Action')


# In[4]:


#Count the differences between two times
from pyspark.sql import functions as f
Duration = (f.unix_timestamp('Qtime_OUT') - f.unix_timestamp('Qtime_IN'))
spTotal = spTotal.withColumn('Duration', Duration).orderBy('Wafer_ID', 'Process_stage')

#Pearson correlation coefficient
spResult = spTotal.groupBy('Process_stage')                   .agg(f.abs(f.corr('yield', 'Duration')).alias('Pearson'))                   .orderBy('Pearson', ascending = False)

#Collect the result from spResult
Results = spResult.select('Process_stage').rdd.flatMap(lambda x: x).collect()
Results = Results[:5]

#Select the data into pandas
dfResults = []

for index in Results:
    dfResults.append(spTotal.where(spTotal.Process_stage == index)                            .select('yield', 'Duration').toPandas())


# In[5]:


#Print the chart
import matplotlib.pyplot as plt

for i in range(len(dfResults)):
    plt.title('Process_stage : ' + str(Results[i]))
    plt.xlabel('Qtime')
    plt.ylabel('Yield')
    plt.scatter(dfResults[i]['Duration'], dfResults[i]['yield'])
    plt.savefig('HW3_%s' %Results[i])
    plt.show()

