#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import numpy as np
import pandas as pd
import requests
import datetime
import calendar 
import warnings
import os
warnings.filterwarnings("ignore")


# In[ ]:


def Save_file(df,name,I):
    path =r"C:\Users\visha\Final Semester Project\CSV Data"
    output_file = os.path.join(path,'%s.csv'%name)
    if I == 1:
        df.to_csv(output_file)
    elif I == 0:
        df.to_csv(output_file,index=False)


# In[ ]:


# df = pd.read_json('https://api.covid19tracking.narrativa.com/api/2020-01-23')


# In[ ]:


# df2 = pd.read_json('https://api.covid19tracking.narrativa.com/api/countries')


# In[ ]:


# list1 = list(df2['countries'])
# df3 = pd.DataFrame(list1)


# In[ ]:


# df4 = pd.read_json('https://api.covid19tracking.narrativa.com/api/2020-03-22/country/afghanistan')


# In[ ]:


# Aa = df4.T['2020-03-22']


# In[ ]:


# Bb = Aa.T['dates']


# In[ ]:


df_raw = pd.read_csv('https://raw.githubusercontent.com/microsoft/Bing-COVID-19-Data/master/data/Bing-COVID19-Data.csv')


# In[ ]:


# df_raw.to_csv("Raw_data_2.csv",index=False)


# In[ ]:


Save_file(df_raw,"Raw_data_2",0)


# In[ ]:


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


# In[ ]:


df,World_data, Country_wise_data = Clean_preprocess(df_raw)


# In[ ]:


# df.to_csv("Cleaned_Raw_data_2.csv",index=False)
# World_data.to_csv("World_data_timeseries.csv",index=False)
# Country_wise_data.to_csv("Country_wise_data_timeseries.csv",index=False)
Save_file(World_data,"World_data_timeseries",0)
Save_file(Country_wise_data,"Country_wise_data_timeseries",0)


# In[ ]:


print("Script Ran Successfully")


# In[ ]:


# df_irl = pd.read_csv('http://opendata-geohive.hub.arcgis.com/datasets/d9be85b30d7748b5b7c09450b8aede63_0.csv?outSR={%22latestWkid%22:3857,%22wkid%22:102100}')


# In[ ]:


# df_irl= df_irl.drop(['OBJECTID', 'ORIGID','IGEasting', 'IGNorthing', 'Lat', 'Long', 'UGI', 'Shape__Area', 'Shape__Length'],axis=1)


# In[ ]:


# df_irl


# In[ ]:


# df_JH_sum = pd.read_csv('Summary_data.csv')

# U = df_JH_sum[df_JH_sum['CountryCode'].isnull()].index.tolist()[0]
# df_JH_sum.loc[U,'CountryCode'] = "NA"
# df_JH_sum.loc[[U]]


# In[ ]:


# GG = list(Country_wise_data['ISO2'].unique())
# GL = list(df_JH_sum['CountryCode'].unique())
# FF = list(set(GG) - set(GL))
# len(FF[1:])

