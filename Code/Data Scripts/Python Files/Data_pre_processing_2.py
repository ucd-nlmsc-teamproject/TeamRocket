#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import requests
import datetime
import calendar 
import warnings
import os
warnings.filterwarnings("ignore")


# In[2]:


def Save_file(df,name,I):
    path =r"C:\Users\visha\Final Semester Project\CSV Data"
    output_file = os.path.join(path,'%s.csv'%name)
    if I == 1:
        df.to_csv(output_file)
    elif I == 0:
        df.to_csv(output_file,index=False)


# In[9]:


df_raw = pd.read_csv('https://raw.githubusercontent.com/microsoft/Bing-COVID-19-Data/master/data/Bing-COVID19-Data.csv')


# In[11]:


Save_file(df_raw,"Raw_data_2",0)


# In[12]:


def Clean_preprocess(df):
    df_1 = pd.DataFrame()
    df_2 = pd.DataFrame()
    df_3 = pd.DataFrame()
    df= df.drop(['ID','ConfirmedChange','DeathsChange', 'RecoveredChange', 'Latitude', 'Longitude','ISO3'],axis=1)
    df = df.fillna(0)
    df = df.rename(columns={'Updated': 'Date', 'Country_Region': 'Country'})
    df_1 = df_1.append(df.loc[df['Country'] == 'Worldwide'])
    df_2 = df_2.append(df.loc[df['Country'] != 'Worldwide'])
    C = df_2['Country'].unique()
    for i in range(len(C)):
        df_0 = df_2[(df_2['Country'] == C[i]) & (df_2['AdminRegion1'] ==0) & (df_2['AdminRegion2'] ==0)]
        df_3 = df_3.append(df_0)
    
    df_3 = df_3.drop(['AdminRegion1','AdminRegion2'],axis=1)
    return df,df_1,df_3


# In[13]:


df,World_data, Country_wise_data = Clean_preprocess(df_raw)


# In[14]:


Save_file(World_data,"World_data_timeseries",0)
Save_file(Country_wise_data,"Country_wise_data_timeseries",0)


# In[15]:


print("Script Ran Successfully")

