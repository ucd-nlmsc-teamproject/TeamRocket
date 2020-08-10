#!/usr/bin/env python
# coding: utf-8

# # Final Semester Project - Covid-19 Chatbot

# ## Data Collection from the API

# In[1]:


import numpy as np
import pandas as pd
import requests
import datetime
from datetime import datetime
import calendar 
import warnings
import os
from http import HTTPStatus
import time
warnings.filterwarnings("ignore")


# In[2]:


def Save_file(df,name,I):
    path =r"C:\Users\visha\Final Semester Project\CSV Data"
    output_file = os.path.join(path,'%s.csv'%name)
    if I == 1:
        df.to_csv(output_file)
    elif I == 0:
        df.to_csv(output_file,index=False)
    elif I == 2:
        df.to_csv(output_file,sep='|')


# In[3]:


def Countries_Global_Summary(url):
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data['Countries'])
    df2 = pd.DataFrame(data['Global'],index=[0])
    Countries = list(df["Slug"])
    Country = list(df['Country'])
    Countries.sort()
    return df, df2, Countries, Country


# In[4]:


Data_Countries, Data_Global, Countries, Country = Countries_Global_Summary("https://api.covid19api.com/summary")


# In[5]:


def Data_collection(Countries):
    Data = pd.DataFrame()
    for i in range(len(Countries)):
        A = Countries[i]
        df = pd.read_json('https://api.covid19api.com/total/country/%s'%A)
        Data = Data.append(df)
        time.sleep(1)
    
    return Data


# In[6]:


Data = Data_collection(Countries)


# In[7]:


Save_file(Data,"Raw_data",0)


# In[8]:


Data_read = pd.read_csv('C:/Users/visha/Final Semester Project/CSV Data/Raw_Data.csv')


# In[9]:


Data_raw_2 = pd.read_csv('C:/Users/visha/Final Semester Project/CSV Data/Country_wise_data_timeseries.csv')


# In[10]:


def Clean_data(df,df2):
    df = df.reset_index()
    df2 = df2.reset_index()
    df= df.drop(['City', 'CityCode', 'CountryCode','Lat','Lon','Province'],axis=1)
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
    df2['Date'] = pd.to_datetime(df2['Date']).dt.strftime('%Y-%m-%d')
    
    for i in range(0, len(df)):
        if (df.loc[i,'Active'] == 0 ) & (df.loc[i, 'Confirmed'] < df.loc[i, 'Deaths'] + df.loc[i, 'Recovered']):
            C = df.loc[i,'Country'] 
            D = df.loc[i,'Date']
            I = df2[(df2['Date'] == D) & (df2['Country'] == C)].index[0]
            R = df2.loc[I,'Recovered']
            I2 = df[(df['Date'] == D) & (df['Country'] == C)].index[0]
            df.loc[I2,'Recovered'] = R

    for i in range(1, len(df)):
        if (df.loc[i, 'Confirmed'] == 0) and (df.loc[i, 'Country'] == df.loc[i-1,'Country']):
            df.loc[i, 'Confirmed'] = df.loc[i-1, 'Confirmed']
        if (df.loc[i, 'Deaths'] == 0) and (df.loc[i, 'Country'] == df.loc[i-1,'Country']):
            df.loc[i, 'Deaths'] = df.loc[i-1, 'Deaths']
        if (df.loc[i, 'Recovered'] == 0)  and (df.loc[i, 'Country'] == df.loc[i-1,'Country']):
            df.loc[i, 'Recovered'] = df.loc[i-1, 'Recovered']

# Addressing the data descrepencies in active cases column

    for i in range(0, len(df)):
        if df.loc[i,'Active'] != df.loc[i, 'Confirmed'] - df.loc[i, 'Deaths'] - df.loc[i, 'Recovered']:
            df.loc[i,'Active'] = df.loc[i, 'Confirmed'] - df.loc[i, 'Deaths'] - df.loc[i, 'Recovered']
    df= df.drop(['index'],axis=1)
    return df


# In[11]:


Data_1 = Clean_data(Data_read,Data_raw_2)


# In[12]:


Data_2_Sum = pd.read_csv('C:/Users/visha/Final Semester Project/CSV Data/Covid_World_Summary.csv')


# In[13]:


def Data_merger(df,df2):
    df = df.drop(['CountryCode', 'NewConfirmed', 'NewDeaths','NewRecovered'],axis=1)
    df = df.rename(columns={'TotalConfirmed': 'Confirmed', 'TotalDeaths': 'Deaths', 'TotalRecovered': 'Recovered'})
    df['Active'] = df['Confirmed'] - (df['Recovered'] + df['Deaths'])
    df_s = df[['Active','Confirmed','Country','Date','Deaths','Recovered']]
    df2 = df2.append(df_s)
    return df2


# In[14]:


Data_1a = Data_merger(Data_2_Sum,Data_1)


# In[15]:


Save_file(Data_1a,"Cleaned_Raw_data",0)


# In[16]:


def Time_Series_Data(df):
    Dates_list =(df['Date'].unique())#To get the list of all unique Dates
    #Creating three new dataframes which will have the rows and different states and columns as all the unique date
    Confirmed_Data = pd.DataFrame(index=Country,columns=(Dates_list))#Used the country as index in each of the dataframe and columns as Dates
    Recovered_Data = pd.DataFrame(index=Country,columns=(Dates_list))
    Deaths_Data = pd.DataFrame(index=Country,columns=(Dates_list))
    Active_Data = pd.DataFrame(index=Country,columns=(Dates_list))
    
    #In this section i have mapped and converted the data into the format defined in the abobve dataframe.
    X = Confirmed_Data.shape[1] #saving the column lenght
    Y = Confirmed_Data.shape[0] #saving the row lenght
    for i in range(Y):
        df_2 = df.loc[df.Country == Country[i]]
        df_2 = df_2.groupby(['Date']).sum()
        df_2T = df_2.T
        R = df_2T.shape[1]
        for j in range(R):
            Ao = df_2T.columns[j]
            for k in range(X):
                Ai = (Confirmed_Data.columns[k])
                if (Ai == Ao):
                    A = df_2T.loc["Confirmed"][Ai]
                    Confirmed_Data[Ai][Country[i]] = A
                    B = df_2T.loc["Recovered"][Ai]
                    Recovered_Data[Ai][Country[i]] = B
                    C = df_2T.loc["Deaths"][Ai]
                    Deaths_Data[Ai][Country[i]] = C
                    D = df_2T.loc["Active"][Ai]
                    Active_Data[Ai][Country[i]] = D
                else:
                    continue
    
    return Confirmed_Data,Recovered_Data,Deaths_Data,Active_Data


# In[17]:


Confirmed_Data,Recovered_Data,Deaths_Data,Active_Data = Time_Series_Data(Data_1a)


# In[18]:


Save_file(Confirmed_Data,"Confirmed_Data",1)
Save_file(Recovered_Data,"Recovered_Data",1)
Save_file(Deaths_Data,"Deaths_Data",1)
Save_file(Active_Data,"Active_Data",1)


# In[19]:


def transpose(df):
    df = df.T
    df.index.name = 'Date'
    return df


# In[20]:


Confirmed_Data_T = transpose(Confirmed_Data)
Recovered_Data_T = transpose(Recovered_Data)
Deaths_Data_T = transpose(Deaths_Data)
Active_Data_T = transpose(Active_Data)


# In[21]:


Save_file(Confirmed_Data_T,"Confirmed_Data_T",2)
Save_file(Recovered_Data_T,"Recovered_Data_T",2)
Save_file(Deaths_Data_T,"Deaths_Data_T",2)
Save_file(Active_Data_T,"Active_Data_T",2)


# In[22]:


def Integrated_data(df,df1,df2,df3,df4):
    Dates_list = ((df['Date'].unique()))
    Data_final = pd.DataFrame(columns=(Dates_list))
    Data_final.insert(0,'Country',0)
    for i in range(len(Country)):
        Confirmed= list(df1.loc[Country[i]])
        Confirmed = [Country[i]] + Confirmed
        Recovered= list(df2.loc[Country[i]])
        Recovered = [Country[i]] + Recovered
        Deaths= list(df3.loc[Country[i]])
        Deaths = [Country[i]] + Deaths
        Active= list(df4.loc[Country[i]])
        Active = [Country[i]] + Active
        Data = pd.DataFrame(columns=(Dates_list))
        Data.insert(0,'Country',0)
        Data_T = Data.T
        Data_T['Confirmed'] = Confirmed
        Data_T['Recovered'] = Recovered
        Data_T['Deaths'] = Deaths
        Data_T['Active'] = Active
        Data_T2 = Data_T.T
        Data_T2['Country'] = Country[i]
        Data_final = Data_final.append(Data_T2)
    return Data_final


# In[24]:


Final_Data = Integrated_data(Data_1a,Confirmed_Data,Recovered_Data,Deaths_Data,Active_Data)


# In[25]:


Save_file(Final_Data,"Final_Data_Combined",1)


# In[26]:


Data_2 = Clean_data(Data_read,Data_raw_2)


# In[27]:


def Date_processing(df):
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
    df_list = list(df['Date'])
    B=[]
    P=[]
    O=[]
    K=[]
    for i in range(len(df_list)):
        A = str(df_list[i])
        datee = datetime.strptime(A,'%Y-%m-%d').date()
        B.append(datee.month)
        T = datetime.strptime(A,'%Y-%m-%d').weekday() 
        P.append(T)
        Y = (calendar.day_name[T]) 
        O.append(Y)
        U = datetime.strptime(A,'%Y-%m-%d').isocalendar()[1]
        K.append(U)
    df['Month'] = B
    df['Day_number'] = P
    df['Day'] = O
    df['Week'] = K
    C = df['Month']
    D=[]
    for i in range(len(C)):
        if C[i] == 1:
            D.append("January")
        elif C[i] == 2:
            D.append("February")
        elif C[i] == 3:
            D.append("March")
        elif C[i] == 4:
            D.append("April")
        elif C[i] == 5:
            D.append("May")
        elif C[i] == 6:
            D.append("June")
        elif C[i] == 7:
            D.append("July")
        elif C[i] == 8:
            D.append("August")
        elif C[i] == 9:
            D.append("Spetember")
        elif C[i] == 10:
            D.append("October")
        elif C[i] == 11:
            D.append("November")
        elif C[i] == 12:
            D.append("December")
    
    df['Month_name'] = D
    
    return df


# In[28]:


Data_date_2 = Date_processing(Data_2)


# In[29]:


Save_file(Data_date_2,"Data_with_new_columns",0)


# In[30]:


def World_Dataset(df,df1,df2,df3,df4):
    Dates_l =(df['Date'].unique())
    I = ['Confirmed','Deaths','Active','Recovered']
    W_D = pd.DataFrame(index=I,columns=(Dates_l))
    V = W_D.shape[1]
    for i in range(V):
        Ao=Dates_l[i]
        tot_1  = list(df1[Ao])
        total_1 = sum(tot_1)
        W_D.loc['Confirmed'][Ao] = total_1
        tot_2  = list(df2[Dates_l[i]])
        total_2 = sum(tot_2)
        W_D.loc['Deaths'][Ao] = total_2
        tot_3  = list(df3[Dates_l[i]])
        total_3 = sum(tot_3)
        W_D.loc['Active'][Ao] = total_3
        tot_4  = list(df4[Dates_l[i]])
        total_4 = sum(tot_4)
        W_D.loc['Recovered'][Ao] = total_4
    
    return W_D
        


# In[31]:


World_Data = World_Dataset(Data_1,Confirmed_Data,Deaths_Data,Active_Data,Recovered_Data)


# In[32]:


World_Data_Summary = World_Data.T


# In[33]:


World_Data_Summary['Dates'] = World_Data_Summary.index


# In[34]:


Save_file(World_Data_Summary,"World_Data_Summary",0)


# In[35]:


print("Script Ran Successfully")

