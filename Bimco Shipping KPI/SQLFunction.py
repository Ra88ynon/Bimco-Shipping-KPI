import pyodbc as p
import os
from dotenv import load_dotenv
load_dotenv()
connStr = os.getenv("connStrW")  # Remove trusted connection and add password
conn = p.connect(connStr)
mycursor = conn.cursor()
def DeleteTable(TableName):
    TableCreate=f"IF OBJECT_ID(N'{TableName}', N'U') IS NOT NULL DELETE  {TableName}"

    mycursor.execute(TableCreate)
    # mycursor.close()
    conn.commit()
    # conn.close()
    print(f"Table {TableName} Created")

def CreateTable(header,TableName):
    TableCreate=f"IF OBJECT_ID(N'{TableName}', N'U') IS NULL CREATE TABLE {TableName}("+",".join([i+" Varchar(MAX)" for i in header])+")"
    #print(TableCreate)
    mycursor.execute(TableCreate)
    # mycursor.close()
    # conn.commit()
    # conn.close()
    print(f"Table {TableName} Created")
def InsertData(header,Data,TableName):
    tem2 = ",".join([i for i in header])
    for line in Data:
        tem=",".join(["NULL" if i is None  else "'"+str(i).replace("'","''")+"'" for i in line])
        tem=tem.replace("'nan'","Null")
        #print(tem2,"--")
        #print(tem,"--")
        mycursor.execute(f"""INSERT INTO {TableName} ({tem2}) VALUES ({tem})""")
    conn.commit()
def selectTable (tableName,Key,Condition):
    if Condition:
        Condition=f"Where {Condition}"
    mycursor.execute(f"""Select {Key} from {tableName} {Condition}""")
    
    return mycursor.fetchall()
def InsertDataDetail(header,Data,TableName):
    for Header, Line in zip(header,Data):
        Variable=",".join([i for i in Header])
        Value=",".join(["'"+str(i).replace("\n","").replace("'","")+"'" for i in Line])
        mycursor.execute(f"""INSERT INTO {TableName} ({Variable}) VALUES ({Value})""")
    print("complete")
    conn.commit()
    conn.close()
