
# coding: utf-8

# In[ ]:


#Initial the spark
import os
import sys

#this part is used for pyspark submit
os.environ['PYSPARK_SUBMIT_ARGS']='--verbose --master=yarn --deploy-mode=client pyspark-shell'

os.environ['JAVA_HOME']='/usr/lib/jvm/java-8-openjdk-amd64/'
os.environ['YARN_CONF_DIR']='/etc/alternatives/hadoop-conf/'

#this line is used for spark1.6
#os.environ['SPARK_HOME']='/opt/cloudera/parcels/CDH/lib/spark'

#this line is used for spark2.2
os.environ['SPARK_HOME']='/opt/cloudera/parcels/SPARK2-2.2.0.cloudera2-1.cdh5.12.0.p0.232957/lib/spark2'

# this line is used for python2.7
#os.environ['PYSPARK_PYTHON']='/usr/bin/python'

#this line is used for python3.5
os.environ['PYSPARK_PYTHON']='/usr/bin/python3'

spark_home = os.environ.get('SPARK_HOME', None)
sys.path.insert(0, os.path.join(spark_home, 'python'))
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.10.4-src.zip'))  
#execfile(os.path.join(spark_home, 'python/pyspark/shell.py'))
exec(open(os.path.join(spark_home, 'python/pyspark/shell.py')).read())


# In[ ]:


#Import to change the types
from pyspark.sql.types import *

#Read the file
spWaferlog = spark.read              .format('csv')              .option('header', 'true')              .load('/user/homework_2/WaferLog.csv') 

spYield = spark.read           .format('csv')           .option('header', 'true')           .load('/user/homework_1/Yield.csv') 

#Change the schema
spYield = spYield.withColumn('yield', spYield['yield'].cast(DoubleType()))

#Rename the title
spYield = spYield.withColumnRenamed('_c0','Wafer_ID')

oldColumns = ['_c0', 'Lot.ID', 'Wafer.ID', 'Process.stage', 'Tool.ID']
newColumns = ['number', 'Lot_ID', 'Wafer_ID', 'Process_stage', 'Tool_ID']

for i in range(len(oldColumns)):
    spWaferlog = spWaferlog.withColumnRenamed(oldColumns[i], newColumns[i])

#Join the data
spData = spYield.join(spWaferlog, 'Wafer_ID')


# In[ ]:


#Import to have the Row
from pyspark.sql import Row

#Evaluate the mean per Process_stage and Tool_ID
spTool = spData.groupBy('Process_stage', 'Tool_ID').mean('yield').orderBy('Process_stage')

#Evaluate the mean per Process_stage
spProcess = spData.groupBy('Process_stage').mean('yield').orderBy('Process_stage')

#Rename the title
spTool = spTool.withColumnRenamed('avg(yield)', 'Average_Tool')
spProcess = spProcess.withColumnRenamed('avg(yield)', 'Average_Process')

#Collect from the spark dataframe
Tool_IDs = spTool.collect()
Process_stages = spProcess.rdd.collectAsMap()

#Counting for the mean gap
Mean_gaps = []

for i in range(len(Tool_IDs)):
    Mean_gaps.append(Row(Process_stage=Tool_IDs[i][0]                        , Tool_ID=Tool_IDs[i][1]                        , Mean_gap=abs(Tool_IDs[i][2] - Process_stages[Tool_IDs[i][0]])))
    
#Put back the dataframe into spark 
spResult = spark.createDataFrame(Mean_gaps)

#Take back the top five value
keys = spResult.orderBy('Mean_gap', ascending = False).take(5)

#Pick the dataframe from the spData
dfResults = []

for i in range(len(keys)):
    dfResults.append(spData.where((spData.Process_stage == keys[i][1]) & (spData.Tool_ID == keys[i][2]))                            .select('yield', 'Process_stage', 'Tool_ID').toPandas())


# In[ ]:


#Plot the box plot
import matplotlib.pyplot as plt

for i in range(len(dfResults)):
    plt.title('Process_stage ' + dfResults[i]['Process_stage'][0] + ' with Tool_ID ' + dfResults[i]['Tool_ID'][0])
    plt.ylabel('Yield')
    plt.boxplot(dfResults[i]['yield'])
    plt.savefig('HW2_' + dfResults[i]['Process_stage'][0] + '_' + dfResults[i]['Tool_ID'][0])
    plt.show()

