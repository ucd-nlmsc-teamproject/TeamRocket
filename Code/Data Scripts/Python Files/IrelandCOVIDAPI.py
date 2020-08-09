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
import datetime
import warnings
warnings.filterwarnings("ignore")


# In[2]:


def Save_file(df,name,I):
    path =r"C:\Users\apurv\Data Science in Python\Project\Datascripts\CSV Data"
    output_file = os.path.join(path,'%s.csv'%name)
    if I == 1:
        df.to_csv(output_file)
    elif I == 0:
        df.to_csv(output_file,index = False)


# ### This is the dataframe that provides the CONFIRMED covid-19 cases till date in Ireland along with population data county-wise
# #### Gives data about 26 counties until the latest updated date

# In[3]:


def get_data(csv):
    df = pd.read_csv(csv)
    df = df.drop(columns = ['ORIGID', 'IGEasting', 'IGNorthing', 'UniqueGeographicIdentifier', 'ConfirmedCovidDeaths',
       'ConfirmedCovidRecovered', 'x', 'y', 'FID']) 
    df = df.rename(columns= {'PopulationCensus16': 'Population','ConfirmedCovidCases': 'ConfirmedCases','TimeStampDate': 'Date','PopulationProportionCovidCases': 'Case by Population'})    
    df['Date'] = pd.to_datetime(df['Date']) 
    df['Date'] = df['Date'].dt.date
    df.fillna(0, inplace = True) 
    return df
    


# In[4]:


csv = 'http://opendata-geohive.hub.arcgis.com/datasets/4779c505c43c40da9101ce53f34bb923_0.csv?outSR={"latestWkid":3857,"wkid":102100}'
 
Ireland_counties_df = get_data(csv)


# In[5]:


Save_file(Ireland_counties_df,"IrelandConfirmedCases",0)


# ### This is the dataframe that provides with all of the attributes surrounding the covid-19 cases data. 
# ### It provides data like what's the number of critical ICU cases, hopsitalized cases by age demographic
# #### Provides all of the statistical data till date, gender related data, reasons of spread of covid like human contact/travel abroad/community 
# 
# ##### Ireland_Overall = pd.read_csv('http://opendata-geohive.hub.arcgis.com/datasets/d8eb52d56273413b84b0187a4e9117be_0.csv?outSR={"latestWkid":3857,"wkid":102100}')
# 

# In[6]:


def Ireland_Overall(url):
    df = pd.read_csv(url)
    df['Date'] = pd.to_datetime(df['Date'])
    df['Date'] = df['Date'].dt.date
    df.fillna(0, inplace = True)
    df['StatisticsProfileDate'] = pd.to_datetime(df['StatisticsProfileDate']).dt.strftime('%Y-%m-%d')
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')


    df1 = df.filter(['Date', 'ConfirmedCovidCases', 'TotalConfirmedCovidCases',
       'ConfirmedCovidDeaths', 'TotalCovidDeaths'], axis = 1)
    df1 = df1.rename(columns = {'ConfirmedCovidCases':'Confirmed', 'TotalConfirmedCovidCases':'TotalConfirmed',
       'ConfirmedCovidDeaths':'Deaths', 'TotalCovidDeaths': 'TotalDeaths' })
    

    df2 = df.drop(columns = ['X', 'Y', 'ConfirmedCovidCases', 'TotalConfirmedCovidCases',
       'ConfirmedCovidDeaths', 'TotalCovidDeaths', 'StatisticsProfileDate', 'CovidCasesConfirmed','HealthcareWorkersCovidCases', 'ClustersNotified','Male', 'Female', 'Unknown', 'Aged1',
       'Aged1to4', 'Aged5to14', 'Aged15to24', 'Aged25to34', 'Aged35to44',
       'Aged45to54', 'Aged55to64', 'Aged65up', 'Median_Age',
       'CommunityTransmission', 'CloseContact', 'TravelAbroad', 'FID'])
    df2 = df2.rename(columns= {'HospitalisedCovidCases': 'HospitalisedCases','RequiringICUCovidCases': 'Critical_ICUCases',
                                                             'HospitalisedAged5': 'Aged5','HospitalisedAged5to14': 'Aged5-14','HospitalisedAged15to24': 'Aged15-24',
                                                             'HospitalisedAged25to34': 'Aged25-34','HospitalisedAged35to44': 'Aged35to44',
                                                             'HospitalisedAged45to54': 'Aged45to54','HospitalisedAged55to64': 'Aged55to64','HospitalisedAged65up': 'Aged65up'})
    
    
    df3 = df.filter(['Date','Male','Female'], axis = 1)
    
    
    df4 = df.filter(['Date','CommunityTransmission', 'CloseContact', 'TravelAbroad'], axis = 1)
    return df, df1, df2, df3, df4
    


# In[7]:


link = 'http://opendata-geohive.hub.arcgis.com/datasets/d8eb52d56273413b84b0187a4e9117be_0.csv?outSR={"latestWkid":3857,"wkid":102100}'
Entire_Ireland, Ireland_All_Stats, Ireland_Hospital_Data, Ireland_Gender, Ireland_Spread = Ireland_Overall(link) 


# In[8]:


Save_file(Entire_Ireland,"Ireland_TimeSeries",0)


# In[9]:


Save_file(Ireland_All_Stats,"IrelandCaseTypes",0)


# In[10]:


def data_preproc(df):
    list_dates = list()
    P = df.index[df['Date'] == '2020-04-24'][0]
    P1 = df.index[df['Date'] == '2020-04-25'][0]
    A = df.iloc[P]['Aged65up']
    df.loc[P1,'Aged65up'] = A
    return df


# In[11]:


Ireland_Hospital_Data = data_preproc(Ireland_Hospital_Data)


# In[12]:


Save_file(Ireland_Hospital_Data,"IrelandHospitalData",0)


# In[13]:


def Data_cleaning(df):
    list_dates = list()
    R = df.index[df['Date'] == '2020-04-22'][0]
    R1 = df.index[df['Date'] == '2020-04-23'][0]
    T = df.index[df['Date'] == '2020-07-03'][0]
    T1 = df.index[df['Date'] == '2020-07-04'][0]
    X = df.iloc[R]['Male']
    Y = df.iloc[R]['Female']
    Z = df.iloc[T]['Female']
    df.loc[R1,'Male'] = X
    df.loc[T1,'Female'] = Z
    df.loc[R1,'Female'] = Y
    
    return df


# In[14]:


Ireland_Gender = Data_cleaning(Ireland_Gender)


# In[15]:


Save_file(Ireland_Gender,"IrelandGenderWise",0)


# In[16]:


Save_file(Ireland_Spread,"IrelandSpreadCategory",0)


# ### This is the dataframe that consist of timeseries data for all the 26 counties in Ireland (for the number of cases recorded)
# 
# ##### AllCounties_timeseries = pd.read_csv('http://opendata-geohive.hub.arcgis.com/datasets/d9be85b30d7748b5b7c09450b8aede63_0.csv?outSR={"latestWkid":3857,"wkid":102100}')
# #### Gives Confirmed cases starting from February until the latest update

# In[17]:


def IrelandCases(url):
    df = pd.read_csv(url)
    df = df.drop(columns = ['OBJECTID', 'ORIGID','IGEasting','PopulationCensus16','Lat', 'Long', 'IGNorthing','UGI','PopulationProportionCovidCases','ConfirmedCovidDeaths',
       'ConfirmedCovidRecovered', 'Shape__Area', 'Shape__Length'])
    df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
    df['TimeStamp'] = df['TimeStamp'].dt.date
    df = df.rename(columns = {'CountyName' : 'County', 'TimeStamp' : 'Date', 'ConfirmedCovidCases':'Confirmed'})
    return df


# In[18]:


IrelandCounties = IrelandCases('http://opendata-geohive.hub.arcgis.com/datasets/d9be85b30d7748b5b7c09450b8aede63_0.csv?outSR={"latestWkid":3857,"wkid":102100}')


# In[19]:


def Time_Series_Data(df):
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
    Dates_list =(df['Date'].unique())
    County = (df['County'].unique())
    Confirmed_Data = pd.DataFrame(index = County,columns = (Dates_list))
    
    X = Confirmed_Data.shape[1] 
    Y = Confirmed_Data.shape[0] 
    for i in range(Y):
        df_2 = df.loc[df.County == County[i]]
        df_2 = df_2.groupby(['Date']).sum()
        df_2T = df_2.T
        R = df_2T.shape[1]
        for j in range(R):
            Ao = df_2T.columns[j]
            for k in range(X):
                Ai = (Confirmed_Data.columns[k])
                if (Ai == Ao):
                    A = df_2T.loc["Confirmed"][Ai]
                    Confirmed_Data[Ai][County[i]] = A
                else:
                    continue
    
    Confirmed_Data = Confirmed_Data.T
    Confirmed_Data.index.name = 'Date'
    
    return Confirmed_Data


# In[20]:


Confirmed_Ireland = Time_Series_Data(IrelandCounties)


# In[21]:


Save_file(Confirmed_Ireland,"CountyWise_TimeSeries_raw",1)


# In[22]:


Confirmed_Data_ireland = pd.read_csv("C:/Users/apurv/Data Science in Python/Project/Datascripts/CSV Data/CountyWise_TimeSeries_raw.csv",index_col=0)


# In[23]:


Confirmed_Data_ireland


# In[24]:


def Clean_data(df):
    index_date = list(df.index)
    cols = list(df.columns)
    for i in range(len(cols)):
        for j in range(len(index_date)-1):
            A = df.loc[index_date[j]][cols[i]]
            B = df.loc[index_date[j+1]][cols[i]]
            C = df.loc[index_date[j-1]][cols[i]]
            if (A > B) & (A > C):
                df.loc[index_date[j]][cols[i]] = C
                
    return df
    


# In[25]:


County_data = Clean_data(Confirmed_Data_ireland)


# In[26]:


Save_file(County_data,"CountyWise_TimeSeries",1)


# In[27]:


def Spread_Rate(df,df2):
    index_date = list(df.index)
    cols = list(df.columns)
    Population = list(df2['Population'])
    df3 = pd.DataFrame(index=index_date,columns=(cols))
    for i in range(len(cols)):
        for j in range(len(index_date)):
            W = df.loc[index_date[j]][cols[i]]
            X = Population[i]
            Y = (round((W/X)*100,3))
            df3.loc[index_date[j]][cols[i]] = Y
    df3.index.name = 'Date'
    return df3


# In[28]:



Ireland_pop_data = pd.read_csv("C:/Users/apurv/Data Science in Python/Project/Datascripts/CSV Data/IrelandConfirmedCases.csv", index_col=0)


# In[29]:


Spread_rate_data = Spread_Rate(County_data,Ireland_pop_data)
Save_file(Spread_rate_data,"Spread_rate_Ireland",1)


# In[30]:


print("Script Ran Successfully")

