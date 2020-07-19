#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Fetching data from COVID-19 API by Yatko API

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
    response = requests.get(link) #fetching data from the API
    data = response.json()
    region = data['data'] #accessing data available in the key: data
    df_region = pd.DataFrame(region['regions']) #creating a df
    covid_df = df_region.T #transpose
    covid_summary = pd.DataFrame(region['summary'],index = ['Worldwide'])   #for the summary data (worldwide)
    return covid_df, covid_summary


# In[4]:


url = "https://api.quarantine.country/api/v1/summary/latest"
rawdata_Yatko, worldwide_summary_Yatko = get_data(url)


# In[5]:


#rawdata_Yatko Yatko_WorldSummary
Save_file(rawdata_Yatko,"Raw_data_Yatko",0)
Save_file(worldwide_summary_Yatko,"Worldwide_Summary_Yatko",0)
# rawdata_Yatko.to_csv(r'C:\Users\apurv\Data Science in Python\Project\AllDumps\Yatko_RawData.csv',index = False)
# worldwide_summary_Yatko.to_csv(r'C:\Users\apurv\Data Science in Python\Project\AllDumps\Yatko_WorldSummary',index = False)


# In[6]:


def Clean_Data(df):
    df = df.drop(columns = ['iso3166a3', 'iso3166numeric', 'change']) #dropping unnecessary columns
    df = df.rename(columns = {'name': 'Country', 'iso3166a2': 'CountryCode'})
    return df


# In[7]:


CleanData_Yatko = Clean_Data(rawdata_Yatko)
Save_file(CleanData_Yatko,"CleanData_Yatko",0)


# In[8]:


df_JH_Summary = pd.read_csv(r'C:\Users\apurv\Data Science in Python\Project\AllDumps\CLEANED_covid_summary_dump.csv')


# In[9]:


#Function to make the Yatko data feasible to integrate with Johns Hopkinns data
#Pre-processing on Yatko 

def Data_integration(df,df2):
    #finding out the countries where null values exist using the CountryCode
    U = df[df['CountryCode'].isnull()].index.tolist()
    df.loc[U,'CountryCode'] = "NA" 
    #renaming some records to match the two dfs easily 
    df2 = df2.replace(to_replace ="USA", value ="United States of America") #replacing USA and UK with their complete names
    df2 = df2.replace(to_replace ="UK", value ="United Kingdom")
    K = df2[df2['CountryCode'].isnull()].index.tolist()
    for i in range(len(K)-1):
        L = df2.loc[K[i],"Country"] #popping countries not required
        if (L == 'Diamond Princess'):
            K.pop(i)
        elif (L == 'Ms Zaandam'):
            K.pop(i)
    
    #handling data for the countries Congo, CAR (Central African Republic), Namibia, Lesotho to match in both the dfs
    
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
    
    df2 = df2.sort_values(by = ['Country'], ascending = True) #sorting the df Country wise
    
    return df2


# In[10]:


Yatko_summary = Data_integration(df_JH_Summary, CleanData_Yatko)
#Yatko_summary.info()


# In[11]:


Save_file(Yatko_summary,"Yatko_summary",0)


# In[12]:


#function to merge both the dataframes

def Merge_data(df,df2):
    M_df = pd.merge(df, df2, on = ["CountryCode"], how = "left") #using inner join on the two dfs to retain all values
    M_df = M_df.drop_duplicates('CountryCode', keep = "first") #deleting the rest duplicates(if any),only keeping first 
#     TP  = df.iloc[[df[df['CountryCode']=='XK'].index.values.astype(int)[0]]] #Handling data for country with countrycode 'XK'
#     TP = TP.rename(columns= {'Country': 'Country_x'})
#     M_df = M_df.append(TP)
    M_df = M_df.fillna(0)
    M_df = M_df.drop(['Country_y', 'active_cases', 'deaths', 'recovered','total_cases'], axis = 1)
    M_df = M_df.rename(columns= {'Country_x': 'Country','critical': 'CriticalCases','death_ratio': 'DeathRatio','recovery_ratio': 'RecoveryRatio', 'tested': 'Total Tests Done'})
    M_df = M_df.sort_values(by=['Country'], ascending = True)
    return M_df


# In[13]:


Merged_data = Merge_data(df_JH_Summary,Yatko_summary)
#Merged_data.info()


# In[14]:


Save_file(Merged_data,"MergedDF_Yatko_JohnsHopkinns",0)
print("Script Ran Successfully!")

