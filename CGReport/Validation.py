import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, date
import json
import pandas as pd
import pandera as pa
import numpy as np

def validation():
   
   url='http://34.197.223.94:8000/download_wsr/?file-names=b4'
   resp=requests.get(url)
   if resp.status_code==200:
       display("Successfully Downloaded")
   else:
       display("error")
        
'''    # data to validate
validation_df = pd.DataFrame({
    "Week_No": [1, 2, 3],
    "Batch Mentor": ["Nirmala Pandey","Nirmala Pandey","Ashutosh"],
    "Learning status": ["NA", "Batch Had completed there Sprint 1", "This week  batch covered  Informatica topics and did practical"],
})

# define schema
schema = pa.DataFrameSchema({
    "Week_No": pa.Column(int, checks=pa.Check.le(10)),
    "Batch Mentor": pa.Column(str),
    "Learning status": pa.Column(str),
})

validated_df = schema(validation_df)
print(validated_df)

wpr_stats()'''
