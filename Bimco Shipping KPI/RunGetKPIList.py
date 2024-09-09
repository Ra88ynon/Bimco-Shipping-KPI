import APIFile as API
from datetime import datetime
import pandas as pd
import SQLFunction as SQL
token=API.GetToken()
quarter="Q"+ str(pd.Timestamp(datetime.now()).quarter-1)

#year=datetime.today().year
#year='2024'
#print(quarter)
yearlist=[i for i in range(datetime.today().year-3,datetime.today().year+1)]
QuarterList=['Q1','Q2','Q3','Q4']
def replace(target):
    list1=["(",")","&","nan","/'","’"]
    list2=['DOC_HOLDER']
    list3=['MANAGEMENT_TYPE']

    for i in list1:
        target=target.replace(i,'')
    if target:
        target="_"+target
    return target
replaceitem=["(",")","&","nan","'",]
#SQL.DeleteTable("BIMCO_API_ShippingKPI")
for year in yearlist:
    for quarter in QuarterList:
        print(f"{year}-{quarter}")
        data=API.GetKPIData(token,f"{year}-{quarter}")
        header=[i.replace('DOC_HOLDER','MANAGEMENT_TYPE') for i in data]
        
        line=[row.tolist() for index, row in data.iterrows()]
        #line=[[str(i).replace('nan','') for i in j] for j in line]
        #print(header)
        #print(header)
        
        header2=['_'.join(str(i).split()) for i in line[0]]
        header2=[str(i).replace('Number_of','NO') for i in header2]
        header2=[str(i).replace('_where_a','') for i in header2]
        header2=[replace(i) for i in header2]
        #print(header2)
        
        #print(header2)
        header=[ str(j)+str(i) for i,j in zip(header2,header)]
        SQL.CreateTable(header,"BIMCO_API_ShippingKPI")
        SQL.InsertData(header,line[1:],"BIMCO_API_ShippingKPI")

