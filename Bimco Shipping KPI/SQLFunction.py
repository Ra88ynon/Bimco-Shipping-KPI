import snowflake.connector as sf
# import pyodbc as p
import os
from dotenv import load_dotenv

load_dotenv()
# connStr = os.getenv("connStrW")  # Remove trusted connection and add password
# conn = p.connect(connStr)

# connStr = os.getenv("WherescapeconnStr")
snowflake_user = os.getenv("snowflake_user")
snowflake_password = os.getenv("snowflake_password")
snowflake_account = os.getenv("snowflake_account")
snowflake_warehouse = os.getenv("snowflake_warehouse")
snowflake_database = os.getenv("snowflake_database")
snowflake_schema = os.getenv("snowflake_schema")
snowflake_role = os.getenv("snowflake_role")
conn = sf.connect(
    user=snowflake_user,
    password=snowflake_password,
    account=snowflake_account,
    database=snowflake_database,
    schema=snowflake_schema,
    warehouse=snowflake_warehouse,
    role=snowflake_role
)

# conn = p.connect(connStr)
mycursor = conn.cursor()


def DeleteTable(TableName):
    TableDelete = f"TRUNCATE TABLE IF EXISTS {TableName}"

    mycursor.execute(TableDelete)
    # mycursor.close()
    # conn.commit()
    # conn.close()
    print(f"Table {TableName} Deleted")


def CreateTable(header, TableName):
    TableCreate = f"CREATE TABLE IF NOT EXIST {TableName}(" + ",".join(
        [i + " Varchar(4000)" for i in header]) + ")"
    print(TableCreate)
    mycursor.execute(TableCreate)

    # mycursor.close()
    # conn.commit()
    # conn.close()
    print(f"Table {TableName} Created")


def InsertData(header, Data, TableName):
    tem2 = ",".join([i for i in header])
    for line in Data:
        tem = ",".join(["NULL" if i is None else "'" + str(i).replace("'", "''") + "'" for i in line])
        tem = tem.replace("'nan'", "Null")
        # print(tem2,"--")
        # print(tem,"--")
        mycursor.execute(f"""INSERT INTO {TableName} ({tem2}) VALUES ({tem})""")
    # conn.commit()


def selectTable(tableName, Key, Condition):
    if Condition:
        Condition = f"Where {Condition}"
    mycursor.execute(f"""Select {Key} from {tableName} {Condition}""")

    return mycursor.fetchall()


def InsertDataDetail(header, Data, TableName):
    for Header, Line in zip(header, Data):
        Variable = ",".join([i for i in Header])
        Value = ",".join(["'" + str(i).replace("\n", "").replace("'", "") + "'" for i in Line])
        mycursor.execute(f"""INSERT INTO {TableName} ({Variable}) VALUES ({Value})""")
    print("complete")
    # conn.commit()
    # conn.close()
