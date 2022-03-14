import gspread
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, date
import json
import pandas as pd
from IPython.display import display
import numpy as np

def read_batch_attendance(file_name):
    file_names = {
        'L1' : 'Attendance sheet_JA-Batch 1',
    }

    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "creds.json", scope)
    client = gspread.authorize(creds)

    file = client.open(file_names[file_name])
    
    sheet = file.get_worksheet(0)

    full_data = sheet.get_all_values()
    data = full_data[2:]
    headers = full_data[0]
    attendance_df = pd.DataFrame(data, columns=headers)
    
    attendance_date_df = attendance_df.drop(
        columns=['','Sr No.', 'Candidate name', 'Phone No.', 'Personal Email ID'], errors='ignore')

    date_series = pd.Series([])
    #print(attendance_date_df.columns)
    for i in attendance_date_df.columns.values:
        date = pd.Timestamp(i)
        day = date.day_name()

        if((day == 'Saturday') or (day == 'Sunday')):
            attendance_date_df[i] = "Weekend"
        elif(date.weekday() <= 5):
            attendance_date_df.loc[attendance_date_df[i].str.contains("P|Present|Yes", case=False), i ] = "Present"
            attendance_date_df.loc[attendance_date_df[i].str.contains("A|Absent| |No", case=False), i ] = "Absent"
            attendance_date_df[i].replace(r'^\s*$', "Absent", regex=True)

        
    print(attendance_date_df)

    return attendance_df


#testing
read_batch_attendance('L1')
