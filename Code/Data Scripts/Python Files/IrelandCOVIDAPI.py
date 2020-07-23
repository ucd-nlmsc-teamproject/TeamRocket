#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Fetching data from Ireland COVID-19 API provided by data.gov.ie

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
       'ConfirmedCovidRecovered', 'x', 'y', 'FID']) #dropping unnecessary columns
    df = df.rename(columns= {'PopulationCensus16': 'Population','ConfirmedCovidCases': 'ConfirmedCases','TimeStampDate': 'Date','PopulationProportionCovidCases': 'Case by Population'})    
    df['Date'] = pd.to_datetime(df['Date']) #converting the date to the required date format
    df['Date'] = df['Date'].dt.date
    df.fillna(0, inplace = True) #filling NaN values
    return df
    


# In[4]:


csv = 'http://opendata-geohive.hub.arcgis.com/datasets/4779c505c43c40da9101ce53f34bb923_0.csv?outSR={"latestWkid":3857,"wkid":102100}'
#This is the link that contains population information and Confirmed cases per county in Ireland 
Ireland_counties_df = get_data(csv)
# Ireland_counties_df


# In[5]:


Save_file(Ireland_counties_df,"IrelandConfirmedCases",0)
# Ireland_counties_df.to_csv('IrelandConfirmedCases.csv', index = False)


# ### This is the dataframe that provides with all of the attributes surrounding the covid-19 cases data. 
# ### It provides data like what's the number of critical ICU cases, hopsitalized cases by age demographic
# #### Provides all of the statistical data till date, gender related data, reasons of spread of covid like human contact/travel abroad/community 
# 
# ##### Ireland_Overall = pd.read_csv('http://opendata-geohive.hub.arcgis.com/datasets/d8eb52d56273413b84b0187a4e9117be_0.csv?outSR={"latestWkid":3857,"wkid":102100}')
# 

# In[6]:


# def Ireland_Overall(url):
#     df = pd.read_csv(url)
#     df['Date'] = pd.to_datetime(df['Date'])
#     df['Date'] = df['Date'].dt.date
#     df.fillna(0, inplace = True)
#     df['StatisticsProfileDate'] = pd.to_datetime(df['StatisticsProfileDate']).dt.strftime('%Y-%m-%d')
#     df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')


    
#     #this is the dataframe that'd consist of all the statistical data(confirmed/recovered/deaths)
#     df1 = df.filter(['Date', 'ConfirmedCovidCases', 'TotalConfirmedCovidCases',
#        'ConfirmedCovidDeaths', 'TotalCovidDeaths', 'ConfirmedCovidRecovered',
#        'TotalCovidRecovered'], axis = 1)
#     df1 = df1.rename(columns = {'ConfirmedCovidCases':'Confirmed', 'TotalConfirmedCovidCases':'TotalConfirmed',
#        'ConfirmedCovidDeaths':'Deaths', 'TotalCovidDeaths': 'TotalDeaths','ConfirmedCovidRecovered':'Recovered',
#        'TotalCovidRecovered':'TotalRecovered' })
    
#     #this is the dataframe that'd consist of all the Hospital related statistical data 
#     df2 = df.drop(columns = ['X', 'Y', 'ConfirmedCovidCases', 'TotalConfirmedCovidCases',
#        'ConfirmedCovidDeaths', 'TotalCovidDeaths', 'ConfirmedCovidRecovered',
#        'TotalCovidRecovered', 'StatisticsProfileDate', 'CovidCasesConfirmed','HealthcareWorkersCovidCases', 'ClustersNotified','Male', 'Female', 'Unknown', 'Aged1',
#        'Aged1to4', 'Aged5to14', 'Aged15to24', 'Aged25to34', 'Aged35to44',
#        'Aged45to54', 'Aged55to64', 'Aged65up', 'Median_Age',
#        'CommunityTransmission', 'CloseContact', 'TravelAbroad',
#        'UnderInvestigation', 'FID'])
#     df2 = df2.rename(columns= {'HospitalisedCovidCases': 'HospitalisedCases','RequiringICUCovidCases': 'Critical_ICUCases',
#                                                              'HospitalisedAged5': 'Aged5','HospitalisedAged5to14': 'Aged5-14','HospitalisedAged15to24': 'Aged15-24',
#                                                              'HospitalisedAged25to34': 'Aged25-34','HospitalisedAged35to44': 'Aged35to44',
#                                                              'HospitalisedAged45to54': 'Aged45to54','HospitalisedAged55to64': 'Aged55to64','HospitalisedAged65up': 'Aged65up'})
    
#     #this dataframe consist of gender related data (number males/females affected)
#     df3 = df.filter(['Date','Male','Female'], axis = 1)
    
#     #this dataframe consist of reasons for the spread of covid amongst people
#     df4 = df.filter(['Date','CommunityTransmission', 'CloseContact', 'TravelAbroad'], axis = 1)
#     return df, df1, df2, df3, df4
    


# In[7]:


def Ireland_Overall(url):
    df = pd.read_csv(url)
    df['Date'] = pd.to_datetime(df['Date'])
    df['Date'] = df['Date'].dt.date
    df.fillna(0, inplace = True)
    df['StatisticsProfileDate'] = pd.to_datetime(df['StatisticsProfileDate']).dt.strftime('%Y-%m-%d')
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')


    
    #this is the dataframe that'd consist of all the statistical data(confirmed/recovered/deaths)
    df1 = df.filter(['Date', 'ConfirmedCovidCases', 'TotalConfirmedCovidCases',
       'ConfirmedCovidDeaths', 'TotalCovidDeaths'], axis = 1)
    df1 = df1.rename(columns = {'ConfirmedCovidCases':'Confirmed', 'TotalConfirmedCovidCases':'TotalConfirmed',
       'ConfirmedCovidDeaths':'Deaths', 'TotalCovidDeaths': 'TotalDeaths' })
    
    #this is the dataframe that'd consist of all the Hospital related statistical data 
    df2 = df.drop(columns = ['X', 'Y', 'ConfirmedCovidCases', 'TotalConfirmedCovidCases',
       'ConfirmedCovidDeaths', 'TotalCovidDeaths', 'StatisticsProfileDate', 'CovidCasesConfirmed','HealthcareWorkersCovidCases', 'ClustersNotified','Male', 'Female', 'Unknown', 'Aged1',
       'Aged1to4', 'Aged5to14', 'Aged15to24', 'Aged25to34', 'Aged35to44',
       'Aged45to54', 'Aged55to64', 'Aged65up', 'Median_Age',
       'CommunityTransmission', 'CloseContact', 'TravelAbroad', 'FID'])
    df2 = df2.rename(columns= {'HospitalisedCovidCases': 'HospitalisedCases','RequiringICUCovidCases': 'Critical_ICUCases',
                                                             'HospitalisedAged5': 'Aged5','HospitalisedAged5to14': 'Aged5-14','HospitalisedAged15to24': 'Aged15-24',
                                                             'HospitalisedAged25to34': 'Aged25-34','HospitalisedAged35to44': 'Aged35to44',
                                                             'HospitalisedAged45to54': 'Aged45to54','HospitalisedAged55to64': 'Aged55to64','HospitalisedAged65up': 'Aged65up'})
    
    #this dataframe consist of gender related data (number males/females affected)
    df3 = df.filter(['Date','Male','Female'], axis = 1)
    
    #this dataframe consist of reasons for the spread of covid amongst people
    df4 = df.filter(['Date','CommunityTransmission', 'CloseContact', 'TravelAbroad'], axis = 1)
    return df, df1, df2, df3, df4
    


# In[8]:


link = 'http://opendata-geohive.hub.arcgis.com/datasets/d8eb52d56273413b84b0187a4e9117be_0.csv?outSR={"latestWkid":3857,"wkid":102100}'
Entire_Ireland, Ireland_All_Stats, Ireland_Hospital_Data, Ireland_Gender, Ireland_Spread = Ireland_Overall(link) 


# In[9]:


# Entire_Ireland


# In[10]:


Save_file(Entire_Ireland,"Ireland_TimeSeries",0)
# Entire_Ireland.to_csv('Ireland_TimeSeries.csv', index = False)


# In[11]:


# Ireland_All_Stats


# In[12]:


Save_file(Ireland_All_Stats,"IrelandCaseTypes",0)
# Ireland_All_Stats.to_csv('IrelandCaseTypes.csv', index = False)


# In[13]:


# Ireland_Hospital_Data


# In[14]:


def data_preproc(df):
    list_dates = list()
    P = df.index[df['Date'] == '2020-04-24'][0]
    P1 = df.index[df['Date'] == '2020-04-25'][0]
    A = df.iloc[P]['Aged65up']
    df.loc[P1,'Aged65up'] = A
    return df


# In[15]:


Ireland_Hospital_Data = data_preproc(Ireland_Hospital_Data)


# In[16]:


Save_file(Ireland_Hospital_Data,"IrelandHospitalData",0)
# Ireland_Hospital_Data.to_csv('IrelandHospitalData.csv', index = False)


# In[17]:


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


# In[18]:


Ireland_Gender = Data_cleaning(Ireland_Gender)


# In[19]:


Save_file(Ireland_Gender,"IrelandGenderWise",0)
# Ireland_Gender.to_csv('IrelandGenderWise.csv', index = False)


# In[20]:


# Ireland_Spread


# In[21]:


Save_file(Ireland_Spread,"IrelandSpreadCategory",0)
# Ireland_Spread.to_csv('IrelandSpreadCategory.csv', index = False)


# ### This is the dataframe that consist of timeseries data for all the 26 counties in Ireland (for the number of cases recorded)
# 
# ##### AllCounties_timeseries = pd.read_csv('http://opendata-geohive.hub.arcgis.com/datasets/d9be85b30d7748b5b7c09450b8aede63_0.csv?outSR={"latestWkid":3857,"wkid":102100}')
# #### Gives Confirmed cases starting from February until the latest update

# In[22]:


def IrelandCases(url):
    df = pd.read_csv(url)
    #This is the dataframe that consist of timeseries data for all the counties in Ireland
    #Gives Confirmed cases starting from February until the latest update
    df = df.drop(columns = ['OBJECTID', 'ORIGID','IGEasting','PopulationCensus16','Lat', 'Long', 'IGNorthing','UGI','PopulationProportionCovidCases','ConfirmedCovidDeaths',
       'ConfirmedCovidRecovered', 'Shape__Area', 'Shape__Length'])
    df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
    df['TimeStamp'] = df['TimeStamp'].dt.date
    df = df.rename(columns = {'CountyName' : 'County', 'TimeStamp' : 'Date', 'ConfirmedCovidCases':'Confirmed'})
    return df


# In[23]:


IrelandCounties = IrelandCases('http://opendata-geohive.hub.arcgis.com/datasets/d9be85b30d7748b5b7c09450b8aede63_0.csv?outSR={"latestWkid":3857,"wkid":102100}')
# IrelandCounties


# In[24]:


def Time_Series_Data(df):
    Dates_list =(df['Date'].unique())#To get the list of all unique Dates
    County = (df['County'].unique())
    #Creating three new dataframes which will have the rows and different states and columns as all the unique date
    Confirmed_Data = pd.DataFrame(index = County,columns = (Dates_list))#Used the country as index in each of the dataframe and columns as Dates

    #In this section i have mapped and converted the data into the format defined in the abobve dataframe.
    X = Confirmed_Data.shape[1] #saving the column lenght
    Y = Confirmed_Data.shape[0] #saving the row lenght
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

    return Confirmed_Data.T


# In[25]:


Confirmed_Ireland = Time_Series_Data(IrelandCounties)


# In[26]:


# Confirmed_Ireland


# In[27]:


# Date = AllCounties_timeseries['TimeStamp'].unique()
# CountyName = AllCounties_timeseries['CountyName'].unique()


# In[28]:


# Counties_Timeseries = pd.DataFrame(index = CountyName, columns = (Date))
# Counties_Timeseries


# In[29]:


Save_file(Confirmed_Ireland,"CountyWise_TimeSeries",1)
# Confirmed_Ireland.to_csv(CountyWise_TimeSeries.csv', index = False)

