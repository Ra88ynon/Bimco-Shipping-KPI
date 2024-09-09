import APIFile as API
from datetime import datetime
import pandas as pd
#import SQLFunction as SQL
token=API.GetToken()
StartQ="Q"+ str(pd.Timestamp(datetime.now()).quarter-1)
EndQ="Q"+ str(pd.Timestamp(datetime.now()).quarter)
StartQ="Q1"
EndQ="Q1"

year=str(datetime.today().year)
StartQuarter='2022'+"-"+StartQ
#EndQuarter='2022'+"-"+EndQ
#print(StartQuarter,EndQuarter)
data=API.getbenchmark(token,StartQuarter,StartQuarter)
data=[i for i in data['benchmarkGroups']]
header=[str(j).replace('group','GroupID').replace('key','keyid') for j in data[0]]
datalist=[ [str(j[i]).replace('None','nan') for i in j] for j in data]
tem=[]
for i in datalist:
    #print(type(i))
    i.append(StartQuarter)
    tem.append(i)
datalist=tem
header.append('FromToQuarter')
print(datalist[0])
print(len(datalist[0]))
#SQL.DeleteTable("BIMCO_API_Benchmark")
#SQL.CreateTable(header,"BIMCO_API_Benchmark")
#SQL.InsertData(header,datalist,"BIMCO_API_Benchmark")
