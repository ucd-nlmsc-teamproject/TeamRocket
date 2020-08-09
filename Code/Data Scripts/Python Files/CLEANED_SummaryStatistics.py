#!/usr/bin/env python
# coding: utf-8

# ### This is the CLEANED Summary Data of all the required Statistics!
# #### It contains the following values for 186 countries
# 
# #### New Confirmed, Total Confirmed
# #### New Deaths, Total Deaths
# #### New Recovered, Total Recovered

# In[1]:


import pandas as pd
import numpy as np
import urllib.request
import os
import csv
import requests
import json


# In[2]:


def Save_file(df,name,I):
    path =r"C:\Users\apurv\Data Science in Python\Project\Datascripts\CSV Data"
    output_file = os.path.join(path,'%s.csv'%name)
    if I == 1:
        df.to_csv(output_file)
    elif I == 0:
        df.to_csv(output_file,index=False)


# In[3]:


def get_data(link):               
    response = requests.get(link) 
    covid_data = response.json()  
    df = pd.DataFrame(covid_data['Countries']) 
    df = df.reindex(sorted(df.columns), axis = 1) 
    df = df.drop(columns = ['Slug','Premium'])  
    df['Date'] = pd.to_datetime(df['Date'])
    df['Date'] = df['Date'].dt.date
    return df
    


# In[4]:


link = 'https://api.covid19api.com/summary'
covid_summary_df = get_data(link)


# In[5]:


Save_file(covid_summary_df,"Covid_World_Summary",0)
print("Script Ran Successfully!")

