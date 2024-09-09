import APIFile as API
from datetime import datetime
import pandas as pd
#import SQLFunction as SQL
token=API.GetToken()

def GetBenchmark(Quarter):
    data=API.getbenchmark(token,Quarter,Quarter)
    data=[i for i in data['benchmarkGroups']]
    header=[str(j).replace('group','GroupID').replace('key','keyid') for j in data[0]]
    datalist=[ [str(j[i]).replace('None','nan') for i in j] for j in data]
    tem=[]
    for i in datalist:
        #print(type(i))
        i.append(Quarter)
        tem.append(i)
    datalist=tem
    header.append('FromToQuarter')
    
    print(pd.DataFrame(datalist,columns=header).head(3))
    #print(len(datalist[0]))
    
    #SQL.CreateTable(header,"BIMCO_API_Benchmark")
    #SQL.InsertData(header,datalist,"BIMCO_API_Benchmark")

#year=str(datetime.today().year)
#StartQuarter='2022'+"-"+StartQ
#EndQuarter='2022'+"-"+EndQ
#print(StartQuarter,EndQuarter)
#SQL.DeleteTable("BIMCO_API_Benchmark")
for year in range(2022, 2025):
    for Quar in range(1,5):
        Quarter=f"{year}-{Quar}"
        GetBenchmark(Quarter)

