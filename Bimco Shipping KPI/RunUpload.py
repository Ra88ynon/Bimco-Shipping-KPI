import SQLFunction as Sql
import pandas as pd
import APIFile as API
import math
import pytz
import datetime as date
import logging
kuala_lumpur_tz = pytz.timezone('Asia/Kuala_Lumpur')
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    filename = 'Logfile.log',
    datefmt='%Y-%m-%d %H:%M:%S')
utc_timestamp = date.datetime.now(kuala_lumpur_tz)
def make_json(csvFilePath, jsonFilePath):#convert file to Json
    # create a dictionary
    data = []
    # Open a csv reader called DictReader
    csvf= pd.read_csv(csvFilePath, encoding='cp1252')
    header=list(csvf.columns)

    print("--",header)
        # Convert each row into a dictionary
        # and add it to data
    for rows in csvf.index:
        tem={}
        for name in header:
            # print(name,rows)
            if type(csvf[name][rows])!=str and math.isnan(csvf[name][rows]):
                tem[name]=""
            else:
                tem[name]=csvf[name][rows]

        data.append(tem)
    # print(data)
    with open(jsonFilePath, 'w', encoding='utf-8') as jsonf:
        jsonf.write(json.dumps(data, indent=4))

Header1=['IMO_NUMBER','VESSEL_NAME','DOC_HOLDER','TYPE','BUSINESS_UNIT','FLEET','QUARTER','SPI001','SPI002','SPI003',
         'SPI004','SPI005','SPI006','SPI007','SPI009','KPI001','KPI002','KPI003','KPI004','KPI005','KPI006','KPI007',
         'KPI008','KPI009','KPI010','KPI011','KPI012','KPI013','KPI014','KPI015','KPI016','KPI017','KPI018','KPI019',
         'KPI020','KPI021','KPI022','KPI023','KPI024','KPI025','KPI026','KPI027','KPI028','KPI029','KPI030','KPI031',
         'KPI032','KPI033','KPI034','KPI035','KPI036','PI001','PI002','PI003','PI004','PI005','PI006','PI007','PI008',
         'PI009','PI010','PI011','PI012','PI013','PI014','PI015','PI016','PI017','PI018','PI019','PI020','PI021','PI022',
         'PI023','PI024','PI025','PI026','PI027','PI028','PI029','PI030','PI031','PI032','PI033','PI034','PI035','PI036',
         'PI037','PI038','PI039','PI040','PI041','PI042','PI043','PI044','PI045','PI046','PI047','PI048','PI049','PI050',
         'PI051','PI052','PI053','PI054','PI055','PI056','PI057','PI058','PI059','PI060','PI061','PI062','PI063','PI064',
         'PI065','PI066','PI067','META001','META002','META003','META004','META005','META006','META007','META008','META010',
         'META014','META015','META016','META017','META018','META019','META021','META022','META023','META024','META025','META026',
         'CUSTOM001','CUSTOM002','CUSTOM003','CUSTOM004','CUSTOM005']
Header2=['IMO No','Vessel','Management Type','','','','','Environmental','Health and Safety','HR Management','Navigational Safety',
         'Operational','Security','Technical','Port State Control','Ballast water management violations','Budget performance',
         'Cadets per ship','Cargo related incidents','CO2 efficiency','Condition of class','Contained spills',
         'Crew disciplinary frequency','Crew planning','Drydocking planning performance','Environmental deficiencies',
         'Failure of critical equipment and systems','Fire and Explosions','Port state control performance',
         'Health and Safety deficiencies','HR deficiencies','Lost Time Injury Frequency','Lost Time Sickness Frequency',
         'Navigational deficiencies','Navigational incidents','NOx efficiency','Officer retention rate','Officer experience rate',
         'Operational deficiencies','Passenger injury ratio','Port state control deficiency ratio','Port state control detention',
         'Releases of substances','Security deficiencies','SOx efficiency','Training days per officer','Ship availability',
         'Vetting deficiencies','Total Recordable Case Frequency','Total Recordable Case Frequency including First Aid Cases',
         'Overdue tasks in PMS','Actual drydocking costs','Actual drydocking duration','Actual unavailability',
         'Agreed drydocking budget','Agreed drydocking duration','Average number of officers employed','Emitted mass of CO2',
         'Emitted mass of NOx','Emitted mass of SOx','Last year’s AAE (Additional Authorized Expenses)',
         'Last year’s actual running costs and accruals','Last year’s running cost budget','Number of absconded crew',
         'Number of allisions','Number of ballast water management violations','Number of beneficial officer terminations',
         'Number of cadets under training with the DOC holder','Number of cargo related incidents',
         'Number of cases where a crew member is sick for more than 24 hours','Number of cases where drugs or alcohol is abused',
         'Number of charges of criminal offences','Number of collisions','Number of conditions of class',
         'Number of contained spills of liquid','Number of seafarers not relieved on time','Number of dismissals',
         'Number of environmental related deficiencies','Number of explosion incidents',
         'Number of failures of critical equipment and systems','Number of fatalities due to work injuries',
         'Number of fatalities due to sickness','Number of fire incidents','Number of groundings',
         'Number of health and safety related deficiencies','Number of HR related deficiencies','Number of logged warnings',
         'Number of lost workday cases','Number of navigational related deficiencies',
         'Number of officer days onboard all ships with the DOC holder','Number of officer experience points',
         'Number of officer terminations from whatever cause','Number of officer trainee man days','Number of officers onboard',
         'Number of operational related deficiencies','Number of passengers injured','Number of permanent partial disabilities',
         'Number of permanent total disabilities (PTD)','Number of PSC deficiencies','Number of PSC inspections',
         'Number of PSC detentions','Number of PSC inspections resulting in zero deficiencies',
         'Number of recorded external inspections','Number of releases of substances to the environment',
         'Number of security related deficiencies','Number of oil spills','Number of unavoidable officer terminations',
         'Number of ships operated under the DOC holder','Number of observations during commercial inspections',
         'Number of commercial inspections','Number of violations of rest hours','Passenger exposure hours',
         'Planned unavailability','Total exposure hours','Transport work','Overdue tasks in PMS','Medical Treatment Cases',
         'First Aid Cases','Length','Breadth','Depth','Draft','DWT','Speed','Entry into Management old','Year build',
         'Class Society','P&I Club','Trading Areas','H&M','Nationality Senior Officers','Nationality Junior Officers',
         'Nationality Ratings','Flag','Country Built','Ship Status','Ship Type (IHS)','EEDI','EEXI','Customer specific data 001',
         'Customer specific data 002','Customer specific data 003','Customer specific data 004','Customer specific data 005']


logging.info('Python function ran at %s', utc_timestamp )
Quarter='2024-Q2'
#Query="select * from SAPHANA_API_GLAccount"
data=Sql.selectTable ("stage_ShippingKPI","*",f"Quarter='{Quarter}'")
logging.info(Quarter)
print(data)
data=[[j for j in i] for i in data]
column=['__metadata','ChartOfAccounts','GLAccount'
                  ,'IsBalanceSheetAccount','GLAccountGroup','CorporateGroupAccount'
                  ,'ProfitLossAccountType','SampleGLAccount','AccountIsMarkedForDeletion'
                  ,'AccountIsBlockedForCreation','AccountIsBlockedForPosting','AccountIsBlockedForPlanning'
                  ,'PartnerCompany','FunctionalArea','CreationDate','CreatedByUser','LastChangeDateTime'
                  ,'GLAccountType','GLAccountExternal','IsProfitLossAccount']
header=[Header2]
data=header+data
data=pd.DataFrame(data,columns=Header1)

#data.to_csv(f"{Quarter}.csv",index=False)
token=API.GetToken()
jsonpath=f"{Quarter}.json"
#df=pd.read_csv(f"{Quarter}.csv", encoding='cp1252')
data.to_json(jsonpath,orient ='records')
Data=API.ValidateUpload(token,jsonpath,Quarter,"Merge")#Perform upload from APIFile function
logging.info(Data)
logging.info("done")
logging.info('Python function end at %s', utc_timestamp )
