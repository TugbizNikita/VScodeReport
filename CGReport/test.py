import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, date
import json
import pandas as pd
import re
import numpy as np
from IPython.display import display
from re import search

def wpr_stats(file_name):
    file_names = {
        'b1':'WPR_JEE FS Devops Cloud(GCP)  - 30-Nov-2021-24-Jan-22',
        'b2' :'WPR_NET Core - 30-Nov-2021-24-Jan-22',
        'b3' :'WPR-JEE with DevOps & Cloud(GCP) Dec 2nd Batch2-Updated on 24-Jan-22',
        'b4':'WPR-BI V5-DB ETL Testing Dec 21st Batch-Updated on 24-Jan-22',
        'b5':'WPR_V&V_SELJ_BP_04-01-22_47_24-Jan-22',
        'b6':'WPR_V&V_UFT_BP_06-01-22_58_24-Jan-22',
        'b7' :'Systems C with Linux Jan 25th Batch2',
        'L1' : 'JA-1-Updated on 25-Jan-22',
        'L2' : 'JEE Full Stack 2.0 with React Batch 2 JR-6',
        'L3' : 'WPR - JR7',
        'L4' : 'Digital CRM SFDC Batch 1',
        'L5' : 'NET Core with Azure',
        'L6' : 'WPR_JR-15', 
        'L8' : 'WPR_JCAWS-8',
        'L9' : 'WPR_JCAWS 6 & 9',
        'L10': 'WPR_JCAWS 10',
        'L11': 'WPR_JCGCP  11',
        'b8' : 'WPR  Systems_C CPP Linux Programming Feb 22nd Batch1',
        'b9' : 'WRP Systems_C CPP Linux Programming Feb 22nd Batch2',
        'C1' : 'WPR CIS Feb 2022',
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
    headers = full_data[1]
    df = pd.DataFrame(data, columns=headers)

    # print(dict(df['W2-Technical'].apply(pd.to_numeric).value_counts()))
    ranges = []
    #data_frame = pd.DataFrame(columns=['Week_No', 'Above Avg', 'Avg', 'Below_Avg', 'DO'])
    
    records = []
    for i in headers:
        
        if i.endswith('-Technical'):
            
            w1 = dict(df[i].value_counts())
            #print(w1)
            ranges = {'Week_No':"", 'Above_Avg': 0, 'Avg': 0, 'Below_Avg': 0, 'DO' : 0, 'NA' : 0, 'Transfer Out' : 0}
            ranges['Week_No'] = str(''.join(filter(lambda i: i.isdigit(), i)))
            # avg_technical_week_wise[i] = round(df[df[i].apply(lambda x: x.isnumeric())][i].apply(pd.to_numeric).mean(), 2)
            for key, value in w1.items():
                key = key.strip()

                if ((key == 'DO') or (key == 'Do') or (key == 'D/O')):
                    ranges['DO'] += int(value)
                #elif ((key.lower() == 'Transfer Out'.lower()) or (key == 'Tranfer out')):
                 #   ranges['Transfer Out'] += int(value)
                elif ((key == 'Transfer Out') or (key == 'Tranfer out') or (key == 'Transfer out') or (key == 'Tranfer Out')):
                    ranges['Transfer Out'] += int(value)    
                elif ((key == 'NA') or (key == '') or (key == 'N/A') or (key == 'Absent')):
                    ranges['NA'] += int(value)
                elif ((float(key) >= 0) and (float(key) < 3)):
                    ranges['Below_Avg'] += int(value)
                elif ((float(key) == 3)):
                    ranges['Avg'] += int(value)

                elif ((float(key) > 3) and (float(key) <= 5)):
                    ranges['Above_Avg'] += int(value)

            records.append(ranges)
       
    df = pd.json_normalize(records)
    print(df)
    return df

wpr_stats('L5')