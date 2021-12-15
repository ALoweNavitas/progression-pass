import pandas as pd
import numpy as np
import os
import sys
import json
from tqdm import trange
from sqlite3 import connect
from googleapiclient.discovery import build
from google.oauth2 import service_account

def main():
    pass

if __name__ == '__main__':
    main()

# Sets the directory
os.chdir(os.getcwd())
dir = os.getcwd()

# Variables
amrData = ''
progresspassDB = ''
keysJSON = ''

# SQL Databases
amrDatabase = connect(amrData)

#  Google sheets setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = keysJSON
credentials = None
credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Opens the dictionary
with open('dictionary.json') as json_file:
    data = json.load(json_file)

qualent = pd.read_csv('qualent3.csv')
qualEntLookup = dict(zip(qualent['Code'], qualent['Label'])) # Pure lazy, couldn't be bothered adding to the dictionary

# Dictionary lists for lookup
disableCodes = data['disableCodes']
ethnicCodes = data['ethnicCodes']
termStartDates = data['termStartDates']
grade_dict = data['grade_dict']

# Bins for splitting the attendance figures
bins = [0,20,40,60,80,100]
labels = ['0 to 20','20 to 40','40 to 60', '60 to 80', '80 to 100']

# This downloads the Module Registration database as a dataframe.
ModReg = '1vg5DwP1ToRY9Rkp-6837x5wBFOu1Mh_-7XOANyTP0KA'
service = build('sheets', 'v4', credentials=credentials)
sheet = service.spreadsheets()
result =  sheet.values().get(spreadsheetId=ModReg, range='ModReg_All!A:AB').execute()
values = result.get('values',[])
df = pd.DataFrame(data=values, index=None)
df.columns = df.iloc[0] # Sets the column headers to the top row
df = df[1:] # Sets the column headers to the top row
df = df[~df['Study Period/RPL'].isin(['14T3','15T1','15T2','15T3','16T1','16T2','21T3']) & df['Program Code'].isin(['AU7M5','FF7M5','NT7M5','NT7MA','WM7MD','GA7M5','CN7M5']) & ~df['Module Status'].isin(['Cancelled'])]

# Drops various columns not required
df.drop(['Student Name', 'Citizenship', 'Financial Standing',
'Academic Standing','Faculty Name', 'Program Name', 'Module Name', 
'Timetabled Lecturer for Module','Is Repeat','Total Risk Points', 'Notes', 'Full Name','Final Result for Module', 'Interim Grade for Module','',
'Results Released', 'Results Finalised','Interim Result for Module'], axis=1, inplace=True)
df['Student Code'] = df['Student Code'].astype(int)

# Mapping data. Works the same way as a VLOOKUP, or index/Match
df['Outcome'] = df['Final Grade'].map(grade_dict)
df['termStartDate'] = df['Study Period/RPL'].map(termStartDates)
df['termStartDate'] = pd.to_datetime(df['termStartDate'])
df['Attendance for Module'] = df['Attendance for Module'].str.rstrip('%')
df['Attendance for Module'] = pd.to_numeric(df['Attendance for Module']) # Converts string to number
df['Attendance for Module'] = df['Attendance for Module'].apply(lambda x: float(x)) # Converts each value into a decimal
df['attendanceBand'] = pd.cut(df['Attendance for Module'], bins=bins, labels=labels) # Takes the attendance figure and places it into one of the bin categories
df['Final Grade'] = df['Final Grade'].replace('',np.nan)
df['attendanceBand'] = df['attendanceBand'].cat.add_categories('No attendance')
df['attendanceBand'] = df['attendanceBand'].fillna('No attendance')
df.replace(np.nan,'21',regex=True, inplace=True)

# Gets AMR data for all students on record since 16T3
amrDf = pd.read_sql("SELECT * from 'amr'", amrDatabase)
amrDf['Student Code'] = amrDf['Student Code'].astype(int)
amrDf['HESA DISABLE'] = amrDf['HESA DISABLE'].map(disableCodes).fillna('Information Not Known')
amrDf['HESA ETHNIC'] = amrDf['HESA ETHNIC'].map(ethnicCodes).fillna('Information Not Known')
amrDf['HESA QUALENT3'] = amrDf['HESA QUALENT3'].map(qualEntLookup).fillna('Information Not Known')
amrDf['Student Gender'].fillna('Unknown', inplace=True)
amrDf['Student Birth Date'] = pd.to_datetime(amrDf['Student Birth Date'])
# df['age'] = df['Student Birth Date'].apply(lambda x: today.year - x.year - ((today.month, today.day) < (x.month, x.day))) # Calculate age

# Brings both sets of data together and matches on student code
dfMerge = pd.merge(df, amrDf, how='left')
dfMerge['ageTerm'] = round((dfMerge['termStartDate'] - dfMerge['Student Birth Date']) / np.timedelta64(1, 'Y'))
dfMerge = dfMerge.drop(['termStartDate','Student Birth Date','Entity Short Name', 'Course'], axis=1)

# The ID and range of the Progression & Pass Sheet linked to Datastudio
progressionsheet = '1jtWdScAQMuE7BQBluJXjTStb2ezXTuiNaiIr1LM9wJU'
service = build('sheets', 'v4', credentials=credentials)
data = []

# Call the Google Sheets API and write data to the sheet
sheet = service.spreadsheets()
data = dfMerge.values.tolist()
def update():
    sheet.values().update(spreadsheetId=progressionsheet, range="Upload!A2", valueInputOption="USER_ENTERED", body={"values":data}).execute()

for i in trange(1, file=sys.stdout, desc='Uploading data'):
    try:
        update()
    except:
        print("Something went wrong")
        pass


