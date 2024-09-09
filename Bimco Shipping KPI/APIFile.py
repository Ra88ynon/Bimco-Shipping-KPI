import requests
import pandas as pd
from io import StringIO
import json
from dotenv import load_dotenv
import os
load_dotenv()
End = os.getenv("ShippingKPI")
#End = 'https://bimcoskpiapiv2-production.azurewebsites.net/api'
cache_key = ""
file_path = os.getenv("Credential")
files = {'File': (file_path, open(file_path, 'rb'), 'application/json')}
def GetToken():
    headers = {'accept': 'text/plain'}
    url=End+"/Token/GenerateToken"
    response = requests.post(url, headers=headers, files=files)
    if response.status_code == 200:
        # Print the response text (JWT token or any other response)
        return(response.text)
    else:
        print("Failed to send data. Status code:", response.status_code)
        print("Response:", response.text)
def GetKPIData(token,Quater):
    url=End+"/MyShips/GetKPIData"
    headers = {
        'accept': '*/*',
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    data = {
        "fileType": "CommaSeparatedCsv",
        "quarter": Quater,
        "businessUnitOrFleet": "0",
        "includeShipAttributes": True,
        "includeCustomData": True,
        "includePerformanceIndicators": True,
        "includeKeyPerformanceIndicators": True,
        "includeKpiGroups": True
    }
    response = requests.post(url, headers=headers, json=data)
    print(response.status_code)
    if response.status_code == 200:
        # Process the CSV content
        csv_data = StringIO(response.content.decode('utf-8'))

        cleaned_lines = []
        for i, line in enumerate(csv_data):
            if i >= 3:  # Assuming the problem starts from line 4
                cleaned_lines.append(line.rstrip(',\n') + '\n')
            else:
                cleaned_lines.append(line)
        cleaned_csv_data = StringIO(''.join(cleaned_lines))

        # Read the cleaned CSV content into a Pandas DataFrame
        try:
            df = pd.read_csv(cleaned_csv_data)
            #print(cleaned_csv_data)
            df.to_csv(f"{Quater}.csv", index = False)
            # Display the DataFrame
            return df
        except pd.errors.ParserError as e:
            print("An error occurred while parsing the CSV:", e)
    else:
        print("Failed to send data. Status code:", response.status_code)
        print("Response:", response.text)
def getbenchmark(token,startQ,endQ):
    url = End + "/Benchmark/GetBenchmark"
    headers = {
        'accept': 'text/plain',
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }


    data = {
        "periodCode": "PreviousQuarter",
        "startQuarter": startQ,
        "endQuarter":endQ ,
        "excludeMyShipsFromIndustryShips": False,
        #"ownFilters": [],
        #"industryFilters": [],
        #"ownEntitySelection": [],
        #"industryEntitySelection": [],
        "displayMode": 0
    }
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        # Pretty print the response JSON
        return(response.json())
    else:
        print("Failed to send data. Status code:", response.status_code)
        print("Response:", response.text)

def ValidateUpload(token,path,Quarter,mode):
    url = End + "/MyShips/ValidateUpload"
    headers = {
        'accept': 'text/plain',
        'Authorization': f'Bearer {token}',}
    files2={
        'Quarter': ('', Quarter),
        'File': (path, open(path, 'rb'),'application/json')}
    print("Validating")
    response = requests.post(url, headers=headers, files=files2)
    if response.status_code == 200:
        response_data=response.json()
        print(response_data)
        
        Upload(token, Quarter, response_data["cacheKey"], mode)#perform upload
        # Pretty print the response JSON
        return response_data
    else:
        print("Failed to send data. Status code:", response.status_code)
        print("Response:", response.text)
        return(response.text)
        
def Upload(token,Quarter,cache_key,mode):
    print("Validated, Uploading...")
    url = End+'/MyShips/Upload'
    headers = {
        'accept': 'text/plain',
        'Authorization': f'Bearer {token}'
    }
    files = {
        'Quarter': ('', Quarter),
        'CacheKey': ('', cache_key),
        'ImportMode': ('', mode)
    }
    print(files)
    # Making the POST request
    response = requests.post(url, headers=headers, files=files)

    # Check if the request was successful
    if response.status_code == 200:
        print("Success:", response.text)
    else:
        print("Failed to send data. Status code:", response.status_code)
        print("Response:", response.text)
def getBenchmarkFilter(Token,vesselImoNumber,Quarter):
    url = 'https://skpiapi-test.azurewebsites.net/api/Benchmark/GetAvailableBenchmarkFilters'
    params = {
        'vesselImoNumber': vesselImoNumber,
        'quarter': Quarter
    }
    headers = {
        'accept': '*/*',
        'Authorization': f'Bearer {Token}'
    }

    response = requests.get(url, headers=headers, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Assuming the response is JSON, parse and print it
        print(json.dumps(response.json(), indent=4))
    else:
        print("Failed to get data. Status code:", response.status_code)
        print("Response:", response.text)

