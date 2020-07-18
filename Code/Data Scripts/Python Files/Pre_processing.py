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


# Data.to_csv("Raw_Data.csv",index=False)


# In[9]:


Data_read = pd.read_csv('C:/Users/visha/Final Semester Project/CSV Data/Raw_Data.csv')


# In[20]:


def Clean_data(df):
    
    df= df.drop(['City', 'CityCode', 'CountryCode','Lat','Lon','Province'],axis=1)
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

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
    return df


# In[21]:


Data_1 = Clean_data(Data_read)


# In[22]:


# Data_1


# In[13]:


# Data_1.to_csv("Cleaned_Raw_data.csv",index=False)


# In[23]:


Save_file(Data_1,"Cleaned_Raw_data",0)


# In[24]:


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


# In[25]:


Confirmed_Data,Recovered_Data,Deaths_Data,Active_Data = Time_Series_Data(Data_1)


# In[ ]:


# Confirmed_Data.to_csv("Confirmed_Data.csv")#Saved the csv to inspect the data

# Recovered_Data.to_csv("Recovered_Data.csv")#Saved the csv to inspect the data

# Deaths_Data.to_csv("Deaths_Data.csv")#Saved the csv to inspect the data

# Active_Data.to_csv("Active_Data.csv")#Saved the csv to inspect the data


# In[26]:


Save_file(Confirmed_Data,"Confirmed_Data",1)
Save_file(Recovered_Data,"Recovered_Data",1)
Save_file(Deaths_Data,"Deaths_Data",1)
Save_file(Active_Data,"Active_Data",1)


# In[27]:


def transpose(df):
    df = df.T
    df.index.name = 'Date'
    return df


# In[28]:


Confirmed_Data_T = transpose(Confirmed_Data)
Recovered_Data_T = transpose(Recovered_Data)
Deaths_Data_T = transpose(Deaths_Data)
Active_Data_T = transpose(Active_Data)


# In[29]:


Save_file(Confirmed_Data_T,"Confirmed_Data_T",1)
Save_file(Recovered_Data_T,"Recovered_Data_T",1)
Save_file(Deaths_Data_T,"Deaths_Data_T",1)
Save_file(Active_Data_T,"Active_Data_T",1)


# In[ ]:


# Confirmed_Data_T.to_csv("Confirmed_Data_T.csv")#Saved the csv to inspect the data

# Recovered_Data_T.to_csv("Recovered_Data_T.csv")#Saved the csv to inspect the data

# Deaths_Data_T.to_csv("Deaths_Data_T.csv")#Saved the csv to inspect the data

# Active_Data_T.to_csv("Active_Data_T.csv")#Saved the csv to inspect the data


# In[ ]:


# path =r"C:\Users\visha\Final Semester Project\Data CSV"
# output_file1 = os.path.join(path,'Confirmed_Data_T.csv')
# Confirmed_Data_T.to_csv(output_file1)


# In[ ]:


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


# In[ ]:


Final_Data = Integrated_data(Data_1,Confirmed_Data,Recovered_Data,Deaths_Data,Active_Data)


# In[ ]:


# Final_Data


# In[ ]:


# Final_Data.to_csv("Final_Data_Combined.csv")#Saved the` csv to inspect the data


# In[ ]:


Save_file(Final_Data,"Final_Data_Combined",1)


# In[ ]:


Data_2 = Clean_data(Data_read)


# In[ ]:


# from datetime import datetime
# TT = Data_2['Date'][0]
# # T_datee = datetime.datetime.strptime(TT,"%Y-%m-%d  %H:%M:%S")
# d = datetime.strptime(TT, '%Y-%m-%d')
# d.month


# In[ ]:


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


# In[ ]:


Data_date_2 = Date_processing(Data_2)


# In[ ]:


# Data_date_2


# In[ ]:


# Data_date_2.to_csv("Data_with_new_columns.csv",index=False)#Saved the csv to inspect the data


# In[ ]:


Save_file(Data_date_2,"Data_with_new_columns",0)


# In[ ]:


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
        


# In[ ]:


World_Data = World_Dataset(Data_1,Confirmed_Data,Deaths_Data,Active_Data,Recovered_Data)


# In[ ]:


World_Data_Summary = World_Data.T


# In[ ]:


World_Data_Summary['Dates'] = World_Data_Summary.index


# In[ ]:


# World_Data_Summary.to_csv("World_Data_Summary.csv",index=False)#Saved the csv to inspect the data


# In[ ]:


Save_file(World_Data_Summary,"World_Data_Summary",0)


# In[ ]:


print("Script Ran Successfully")


# In[ ]:


# Data_Sum_country = pd.read_csv('CLEANED_covid_summary_dump.csv')


# In[ ]:


# def top_five_Confirmed_cases(df):
#     Final_df = df.sort_values(by=['TotalConfirmed'], ascending=False)
#     Final_df = Final_df.drop(['NewConfirmed','NewDeaths', 'NewRecovered','TotalDeaths','TotalRecovered'],axis=1)
#     Top_confirmed_con_details = Final_df.head()
#     return Top_confirmed_con_details


# In[ ]:


# Top_confirmed_country_details = top_five_Confirmed_cases(Data_Sum_country)


# In[ ]:


# Top_confirmed_country_details.to_csv("Top_confirmed_country_details.csv",index=False)#Saved the csv to inspect the data


# In[ ]:


# def top_five_Recovered_cases(df):
#     Final_df1 = df.sort_values(by=['TotalRecovered'], ascending=False)
#     Final_df1 = Final_df1.drop(['NewConfirmed','NewDeaths', 'NewRecovered','TotalDeaths','TotalConfirmed'],axis=1)
#     Top_recovered_con_details = Final_df1.head()
#     return Top_recovered_con_details


# In[ ]:


# Top_recovered_country_details = top_five_Recovered_cases(Data_Sum_country)


# In[ ]:


# Top_recovered_country_details


# In[ ]:


# Top_recovered_country_details.to_csv("Top_recovered_country_details.csv",index=False)#Saved the csv to inspect the data


# In[ ]:


# def top_five_Deaths_cases(df):
#     Final_df2 = df.sort_values(by=['TotalDeaths'], ascending=False)
#     Final_df2 = Final_df2.drop(['NewConfirmed','NewDeaths', 'NewRecovered','TotalConfirmed','TotalRecovered'],axis=1)
#     Top_deaths_con_details = Final_df2.head()
#     return Top_deaths_con_details


# In[ ]:


# Top_deaths_country_details = top_five_Deaths_cases(Data_Sum_country)


# In[ ]:


# Top_deaths_country_details


# In[ ]:


# Top_deaths_country_details.to_csv("Top_deaths_country_details.csv",index=False)#Saved the csv to inspect the data

