#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import urllib.request
import os
import csv
import requests
import json
import warnings
warnings.filterwarnings("ignore")


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
    data = response.json()
    region = data['data'] 
    df_region = pd.DataFrame(region['regions']) 
    covid_df = df_region.T 
    covid_summary = pd.DataFrame(region['summary'],index = ['Worldwide'])   
    return covid_df, covid_summary


# In[4]:


url = "https://api.quarantine.country/api/v1/summary/latest"
rawdata_Yatko, worldwide_summary_Yatko = get_data(url)


# In[5]:



Save_file(rawdata_Yatko,"Raw_data_Yatko",0)
Save_file(worldwide_summary_Yatko,"Worldwide_Summary_Yatko",0)


# In[6]:


def Clean_Data(df):
    df = df.drop(columns = ['iso3166a3', 'iso3166numeric', 'change']) 
    df = df.rename(columns = {'name': 'Country', 'iso3166a2': 'CountryCode'})
    return df


# In[7]:


CleanData_Yatko = Clean_Data(rawdata_Yatko)
Save_file(CleanData_Yatko,"CleanData_Yatko",0)


# In[8]:


df_JH_Summary = pd.read_csv(r'C:\Users\apurv\Data Science in Python\Project\Datascripts\CSV Data\Covid_World_Summary.csv')


# In[9]:


def Data_integration(df,df2):
    
    U = df[df['CountryCode'].isnull()].index.tolist()
    df.loc[U,'CountryCode'] = "NA" 
    
    df2 = df2.replace(to_replace ="USA", value ="United States of America") 
    df2 = df2.replace(to_replace ="UK", value ="United Kingdom")
    K = df2[df2['CountryCode'].isnull()].index.tolist()
    for i in range(len(K)-1):
        L = df2.loc[K[i],"Country"] 
        if (L == 'Diamond Princess'):
            K.pop(i)
        elif (L == 'Ms Zaandam'):
            K.pop(i)
    
    
    for i in range(len(K)):
        E = df2.loc[K[i],'Country']
        Y = df[df['Country']==E].index.values.astype(int)
        if (E == 'DRC'):
            df2.loc[K[i],'Country'] = 'Congo (Kinshasa)'
        elif (E == 'Car'):
            df2.loc[K[i],'Country'] = 'Central African Republic'
    
    for i in range(len(K)):
        N = df2.loc[K[i],'Country']
        M = df[df['Country']==N].index.values.astype(int)
        if (N == 'Congo (Kinshasa)'):
            df2.loc[K[i],'CountryCode'] = df.loc[M[0],'CountryCode']
        elif (N == 'Central African Republic'):
            df2.loc[K[i],'CountryCode'] = df.loc[M[0],'CountryCode']
        elif (N == 'Namibia'):
            df2.loc[K[i],'CountryCode'] = df.loc[M[0],'CountryCode']
        elif (N == 'Lesotho'):
            df2.loc[K[i],'CountryCode'] = df.loc[M[0],'CountryCode']
        
    J = df2[df2['CountryCode'].isnull()].index.tolist()
    for i in range(len(J)):
        df2 = df2.drop(J[i], axis = 'index')
    
    df2 = df2.sort_values(by = ['Country'], ascending = True) 
    
    return df2


# In[10]:


Yatko_summary = Data_integration(df_JH_Summary, CleanData_Yatko)


# In[11]:


Save_file(Yatko_summary,"Yatko_summary",0)


# In[12]:


def Merge_data(df,df2):
    M_df = pd.merge(df, df2, on = ["CountryCode"], how = "left") 
    M_df = M_df.drop_duplicates('CountryCode', keep = "first") 

    M_df = M_df.fillna(0)
    M_df = M_df.drop(['Country_y', 'active_cases', 'deaths', 'recovered','total_cases'], axis = 1)
    M_df = M_df.rename(columns= {'Country_x': 'Country','critical': 'CriticalCases','death_ratio': 'DeathRatio','recovery_ratio': 'RecoveryRatio', 'tested': 'Total Tests Done'})
    M_df = M_df.sort_values(by=['Country'], ascending = True)
    return M_df


# In[13]:


Merged_data = Merge_data(df_JH_Summary,Yatko_summary)


# In[14]:


Save_file(Merged_data,"MergedDF_Yatko_JohnsHopkinns",0)
print("Script Ran Successfully!")

