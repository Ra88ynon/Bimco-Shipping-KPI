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
StartQuarter='2024'+"-"+StartQ
#EndQuarter='2022'+"-"+EndQ
#print(StartQuarter,EndQuarter)
data=API.getBenchmarkFilter(token,"",StartQuarter)
print(data)

