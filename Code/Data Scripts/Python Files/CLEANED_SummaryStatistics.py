#!/usr/bin/env python
# coding: utf-8

# ### This is the CLEANED Summary Data of all the required Statistics!
# #### It contains the following values for 186 countries
# 
# #### New Confirmed, Total Confirmed
# #### New Deaths, Total Deaths
# #### New Recovered, Total Recovered

# In[1]:


#Fetching data from covid-19 API by Johns Hopkinns

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


def get_data(link):               #defining a function to get covid19 data
    response = requests.get(link) #using the get method here
    covid_data = response.json()  #extracting the data
    df = pd.DataFrame(covid_data['Countries']) #creating a df from the data above and selecting the Key: Countries
    df = df.reindex(sorted(df.columns), axis = 1) #re-arranging the columns for enhanced readability
    df = df.drop(columns = ['Slug','Premium'])  #dropping the column SLUG
    df['Date'] = pd.to_datetime(df['Date'])
    df['Date'] = df['Date'].dt.date
    return df
    


# In[4]:


link = 'https://api.covid19api.com/summary'
covid_summary_df = get_data(link)


# In[5]:


# covid_summary_df


# In[6]:


#covid_summary_df.info() #All the columns are clean without any missing values


# In[7]:


# #covid_summary_df.to_csv(r'C:\Users\apurv\Data Science in Python\8thJuly_CLEANED_covid_summary_dump.csv')
# covid_summary_df.to_csv(r'C:\Users\apurv\Data Science in Python\Project\AllDumps\CLEANED_covid_summary_dump.csv')


# In[8]:


Save_file(covid_summary_df,"Covid_World_Summary",0)
print("Script Ran Successfully!")


# In[9]:


# covid_summary_df.info()

