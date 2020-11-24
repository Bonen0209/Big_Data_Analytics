
# coding: utf-8

# In[1]:


#Read the data from csv
import csv

contents = []
labels = []

with open('./Yield.csv', 'r') as label_csv:

    for data in csv.reader(label_csv):
        labels.append(data)

with open('./WAT.csv', 'r') as content_csv:

    for data in csv.reader(content_csv):
        contents.append(data)

#slice the title
labels = labels[1:]
contents = contents[1:]

#Combine two lists
datas = []
for data in range(len(labels)):
    datas.append(labels[data] + contents[data])


# In[2]:


#Transpose the array
import numpy as np

dataArray = np.array(datas)
dataArray = dataArray.transpose()
dataArray = dataArray[1:]
#dataArray.astype(np.float)


# In[3]:


#Count
import heapq
pearsons = []

for data in range(len(dataArray)):
    if data != 0 and data != 1 and data != 2:
        pearsons.append(abs(np.corrcoef(dataArray[data].astype(np.float), dataArray[0].astype(np.float))[0, 1]))
    else:
        pearsons.append(0)

maxs = heapq.nlargest(10, enumerate(pearsons), key=lambda x: x[1])


# In[4]:


#Show the chart
import matplotlib.pyplot as plt


# In[5]:


plt.scatter(dataArray[maxs[0][0]].astype(np.float), dataArray[0].astype(np.float))
plt.show()


# In[6]:


plt.scatter(dataArray[maxs[1][0]].astype(np.float), dataArray[0].astype(np.float))
plt.show()


# In[7]:


plt.scatter(dataArray[maxs[2][0]].astype(np.float), dataArray[0].astype(np.float))
plt.show()


# In[8]:


plt.scatter(dataArray[maxs[3][0]].astype(np.float), dataArray[0].astype(np.float))
plt.show()


# In[9]:


plt.scatter(dataArray[maxs[4][0]].astype(np.float), dataArray[0].astype(np.float))
plt.show()


# In[10]:


plt.scatter(dataArray[maxs[5][0]].astype(np.float), dataArray[0].astype(np.float))
plt.show()


# In[11]:


plt.scatter(dataArray[maxs[6][0]].astype(np.float), dataArray[0].astype(np.float))
plt.show()


# In[12]:


plt.scatter(dataArray[maxs[7][0]].astype(np.float), dataArray[0].astype(np.float))
plt.show()


# In[13]:


plt.scatter(dataArray[maxs[8][0]].astype(np.float), dataArray[0].astype(np.float))
plt.show()


# In[14]:


plt.scatter(dataArray[maxs[9][0]].astype(np.float), dataArray[0].astype(np.float))
plt.show()

