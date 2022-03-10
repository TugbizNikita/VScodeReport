import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, date
import json
import pandas as pd
import numpy as np
from IPython.display import display


def read_data(event=None, context=None):

    event_body = event['body']
    event_body = (json.loads(event_body))

    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "creds.json", scope)
    client = gspread.authorize(creds)

    file = client.open(event_body['file-name'])
    sheet = getattr(file, event_body['sheet-name'])

    formatted_filter_date = None
    try:
        filter_date = event_body['date']
        formatted_filter_date = datetime.strptime(
            filter_date, '%d/%m/%Y').date()
    except:
        pass

    result = sheet.get_all_records()
    response = {
        'statusCode': 200,
        'body': json.dumps({
            'result': filter_by_timestamp(result, formatted_filter_date)
        })
    }

    return response


def filter_by_timestamp(result, filter_date):
    if filter_date == None:
        filter_date = date.today()
    today_present_students = []
    for i in result:
        date_time_obj = datetime.strptime(
            i['Timestamp'], '%m/%d/%Y %H:%M:%S').date()
        if date_time_obj == filter_date:
            today_present_students.append(i)

    return today_present_students


def read_wpa_report(file_name, download):
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
    avg_technical_week_wise = {}
    above_avg_technical_week_wise = {}
    below_avg_technical_week_wise = {}
    # print(headers)

    for i in headers:
        if i.endswith('-Technical'):
            avg_technical_week_wise[i] = round(df[df[i].apply(lambda x: x.isnumeric())][i].apply(pd.to_numeric).mean(), 2)#todo..check this logic error in b7w3


    if download == True:
        CFMG_Code = df['CFMG Batch Code']
        CFMG_Code = CFMG_Code.get(0)
        print(CFMG_Code)
        result = {'wpr_data_df': df, 'sheet_name': CFMG_Code}
    else:
        #df = df.rename({'CFMG_Code': 'CFMG Batch Code'}, axis=1)
        result = {'weeklyAvgStats': avg_technical_week_wise, 'allData': df.to_dict('records')}

    
    # print(avg_technical_week_wise)
    #return {'weeklyAvgStats': avg_technical_week_wise, 'allData': df.to_dict('records')}

    return result

#read_wpa_report('L1', False)

# print(json.dumps(read_wpa_report(event={'body': '{"Name": "WPR-BI V5-DB ETL Testing Dec 21st Batch-Updated on 24-Jan-22", "WS_Number": "0", "emp_id": "all", "emp_col_number": "5"}'}, context=None), indent=4))

def avg_finder(x):
    mark_list = []
    for i in range(8):
        # print(f'NV{i+1}')
        # print(x)
        if f'NV{i+1}' in x:
            if x[f'NV{i+1}'] != 0:
                mark_list.append(x[f'NV{i+1}'])
        else:
            break
    if len(mark_list) == 0:
        return 0
    else:
        return round(sum(mark_list)/len(mark_list), 2)


def read_consolidated_report(file_name, download):
    # event_body = event['body']
    # event_body = (json.loads(event_body))
    df_list_of_nv = []
    file_names = {
        'b1':  'JEE FS Devops Cloud(GCP) 48days - 30-Nov-2021',
        'b2':  '.NET Core',
        'b3':  'JEE FS Devops Cloud(GCP) 48days - 02-Dec-2021',
        'b4':  'CG - BI - V5 DB ETL Testing',
        'b5':  'V&V - Automation Testing (Selenium+Java)',
        'b6':  'UFT+C#+VB Script',
        'b7':  'Systems C with Linux',
        'L1':  'LS-JA-1,2',
        'L2' : 'JEE Full Stack 2.0 with React CAMP Batch',
        'L3' : 'JEE Full Stack 2.0 with React CAMP Batch 25-Jan-22 JR-7',
        'L4' : 'Digital CRM SFDC CAMP Batch 27-Jan-22 SFDC-1',
        'L5' : 'NET Core with Azure CAMP Batch 24-Jan-22 NCA-3',
        'L6' : 'JEE Full Stack 2.0 with React Batch 15',
        'L7' : 'JCAWS 6',
        'L8' : 'JCAWS 8',
        'L9' : 'JCAWS-9',
        'L10': 'JCAWS-10',
        'L11': 'JCGCP-11',
        'b8' : 'Consolidated Report Systems_C CPP Linux Programming Feb 22nd Batch1',
        'b9' : 'Consolidated Report Systems_C CPP Linux Programming Batch 2',
        'C1' : 'Consolidated Report CIS Feb 2022',
    }
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "creds.json", scope)
    client = gspread.authorize(creds)

    # file = client.open('UFT+C#+VB Script')
    # file = client.open('.NET Core')
    # file = client.open('JEE FS Devops Cloud(GCP) 48days - 30-Nov-2021')
    file = client.open(file_names[file_name])
    
    m1_imp_report = read_m1_improvement_report(file=file)

    is_improvement_empty = m1_imp_report['is_Empty']
    improvement_stats = 'No Data'
    if is_improvement_empty == False:
        if m1_imp_report['stats']['Exam_Details'] != None:
            improvement_stats = m1_imp_report['stats']['Exam_Details']
        improvement_df = m1_imp_report['df']
        improvement_stats = m1_imp_report['stats']

    shadow_report = read_shadow_project(file=file)
    is_shadow_empty = shadow_report['is_Empty']
    shadow_stats = 'No Data'
    if is_shadow_empty == False:
        if shadow_report['stats']['Exam_Details'] != None:
            shadow_stats = shadow_report['stats']['Exam_Details']
        shadow_df = shadow_report['df']
        shadow_stats = shadow_report['stats']

    M1_sheet = file.worksheet('CG_M1')

    full_data = M1_sheet.get_all_values()
    exam_details_list = full_data[0:9]
    data = full_data[10:]
    headers = full_data[9]
    df = pd.DataFrame(data, columns=headers)
    df['Capgemini Email ID'] = df['Capgemini Email ID'].str.upper() # this is M1 df

    M1_df = pd.DataFrame()
    M1_df['Email Id'] = df['Capgemini Email ID']
    M1_df['EmpID'] = df['EmpID']
    M1_df['M1'] = df['TOTAL']
    

    NV_Stats = {'NV1': None, 'NV2': None, 'NV3': None, 'NV4': None,
                'NV5': None, 'NV6': None, 'NV7': None, 'NV8': None}
    nv_all_exam_data = pd.DataFrame()
    for i in NV_Stats:
        NV_Stats[i] = read_nv_report(file.worksheet(i), i, df_list_of_nv)

    nv_all_exam_data = pd.concat(df_list_of_nv)
    nv_all_exam_data = nv_all_exam_data.sort_values('Email Id')

    nv_all_exam_data = nv_all_exam_data.drop(
        columns=['Points received', 'Points scored', 'Is Passed', 'First Name', 'Last Name'], errors='ignore')
    nv_all_exam_data = nv_all_exam_data.pivot_table(
        'Percentage', ['Email Id'], 'Exam Name')
    nv_all_exam_data = nv_all_exam_data.fillna(0)
    nv_all_exam_data['Avg_Of_NV'] = nv_all_exam_data.apply(
        lambda row: avg_finder(row), axis=1)
    

    nv_all_exam_data_with_m1 = pd.merge(
        M1_df, nv_all_exam_data, on='Email Id', how='left')


    
    nv_all_exam_data_with_m1['Difference_M1_and_NV'] = nv_all_exam_data_with_m1['M1'].apply(pd.to_numeric, errors='coerce') - \
        nv_all_exam_data_with_m1['Avg_Of_NV']
    if is_improvement_empty == False:
        nv_all_exam_data_with_m1 = pd.merge(nv_all_exam_data_with_m1, improvement_df, on='Email Id', how='left')
    if is_shadow_empty == False:
        nv_all_exam_data_with_m1 = pd.merge(nv_all_exam_data_with_m1, shadow_df, on='Email Id', how='left')

    name_email_df = pd.DataFrame()
    name_email_df['Name'] = df['Name']
    name_email_df['Email Id'] = df['Capgemini Email ID']

    nv_all_exam_data = pd.merge(
        name_email_df, nv_all_exam_data_with_m1, on='Email Id', how='left')

    
    # nv_all_exam_data.to_excel("output.xlsx",
            #  sheet_name='Sheet_name_1')
    sprint_1_data = 'No Data'
    sprint_2_data = 'No Data'

    sprint_1 = read_sprint(file, 'Sprint_1')
    if sprint_1['status'] == 'success':
        sprint_1_data = sprint_1['Stats']
        sprint_1_df = sprint_1['df'].rename({'Marks': 'Sprint_1'}, axis=1)
        nv_all_exam_data = pd.merge(
            nv_all_exam_data, sprint_1_df, on='Email Id', how='left')

    sprint_2 = read_sprint(file, 'Sprint_2')
    if sprint_2['status'] == 'success':
        sprint_2_data = sprint_2['Stats']
        sprint_2_df = sprint_2['df'].rename({'Marks': 'Sprint_2'}, axis=1)
        nv_all_exam_data = pd.merge(
            nv_all_exam_data, sprint_2_df, on='Email Id', how='left')


    nv_all_exam_data = nv_all_exam_data.rename(
        columns={'Email Id': 'Email_Id'})

    b_info = file.worksheet('B_Info')
    t_info = file.worksheet('T_Info')

    t_info = t_info.get_all_records()

    b_info_list = b_info.get_all_values()[0:12]
    b_info = {}
    for i in range(len(b_info_list)):
        if b_info_list[i][0] != '':
            b_info[b_info_list[i][0]] = b_info_list[i][1]


    nv_all_exam_data_json = nv_all_exam_data.round(2).to_dict('records')

    exam_details = {}
    for i in range(len(exam_details_list)):
        if exam_details_list[i][0] != '':
            exam_details[exam_details_list[i][0]] = exam_details_list[i][1]

    Pass = 0  # greater than 60
    fail = 0  # less than 60 and greater than 0 or 0
    drop_out = 0  # status = Drop Out
    absent = 0  # status =Absent
    not_able_to_submit = 0  # status = Not able to submit
    avg_pass_mark = 0
    avg_fail_mark = 0

    drop_out = len(df.loc[df['Status'] == 'Drop Out'].index)
    df = df[df.Status != 'Drop Out']

    absent = len(df.loc[df['Status'] == 'Absent'].index)
    df = df[df.Status != 'Absent']

    not_able_to_submit = len(
        df.loc[df['Status'] == 'Not able to submit'].index)
    df = df[df.Status != 'Not able to submit']

    fail_df = df.loc[df['Improvement_counter'] == '1']
    fail_df["TOTAL"] = fail_df["TOTAL"].apply(pd.to_numeric)
    avg_fail_mark = fail_df['TOTAL'].mean()
    fail = len(fail_df.index)
    df = df[df.Improvement_counter != 1]

    Pass_df = df.loc[df['Improvement_counter'] == '0']
    avg_pass_mark = Pass_df['TOTAL'].apply(pd.to_numeric).mean()
    Pass_df['TOTAL'] = Pass_df['TOTAL'].apply(pd.to_numeric)
    Pass = len(Pass_df.index)

    pass_percentage = {
        "60-70": 0,
        "70-80": 0,
        "80-90": 0,
        "90-100": 0,
    }

    fail_percentage = {
        "0-25": 0,
        "25-50": 0,
        "50-55": 0,
        "55-59.99": 0,
    }
    if Pass_df.empty == False:

        pass_percentage = {
            "60-70": round(len(
            Pass_df.loc[(Pass_df['TOTAL'].apply(pd.to_numeric) >= 60) & (Pass_df['TOTAL'].apply(pd.to_numeric) < 70)].index) / (Pass_df.shape[0]) * 100, 2),
            "70-80": round(len(
            Pass_df.loc[(Pass_df['TOTAL'].apply(pd.to_numeric) >= 70) & (Pass_df['TOTAL'].apply(pd.to_numeric) < 80)].index) / (Pass_df.shape[0]) * 100, 2),
            "80-90": round(len(
            Pass_df.loc[(Pass_df['TOTAL'].apply(pd.to_numeric) >= 80) & (Pass_df['TOTAL'].apply(pd.to_numeric) < 90)].index) / (Pass_df.shape[0]) * 100, 2),
            "90-100": round(len(
            Pass_df.loc[(Pass_df['TOTAL'].apply(pd.to_numeric) >= 90) & (Pass_df['TOTAL'].apply(pd.to_numeric) <= 100)].index) / (Pass_df.shape[0]) * 100, 2),
        }
    
    if fail_df.empty == False:

        fail_percentage = {
            "0-25": round(len(
            fail_df.loc[(fail_df['TOTAL'].apply(pd.to_numeric) >= 0) & (fail_df['TOTAL'].apply(pd.to_numeric) < 25)].index) / (fail_df.shape[0]) * 100, 2),
            "25-50": round(len(
            fail_df.loc[(fail_df['TOTAL'].apply(pd.to_numeric) >= 25) & (fail_df['TOTAL'].apply(pd.to_numeric) < 50)].index) / (fail_df.shape[0]) * 100, 2),
            "50-55": round(len(
            fail_df.loc[(fail_df['TOTAL'].apply(pd.to_numeric) >= 50) & (fail_df['TOTAL'].apply(pd.to_numeric) < 55)].index) / (fail_df.shape[0]) * 100, 2),
            "55-59.99": round(len(
            fail_df.loc[(fail_df['TOTAL'].apply(pd.to_numeric) >= 55) & (fail_df['TOTAL'].apply(pd.to_numeric) < 60)].index) / (fail_df.shape[0]) * 100, 2),
        }

    if download == True:
        result = {'df':nv_all_exam_data, 'sheet_name': b_info['CFMG Code']}
    else:
        result = {'M1': {'Pass': Pass, 'Fail': fail, 'Drop_Out': drop_out, 'Absent': absent, 'Not_Able_To_Submit': not_able_to_submit, 'Avg_Pass_Mark': round(avg_pass_mark, 2),
                        'Avg_Fail_Mark': round(avg_fail_mark, 2), 'Pass_Percentage': pass_percentage, 'Fail_Percentage': fail_percentage},'Improvent_M1': improvement_stats, 'NV': NV_Stats, 'Sprint_1': sprint_1_data, 'Sprint_2': sprint_2_data, 'All_Exam_Data': nv_all_exam_data_json, 'T_info': t_info, "B_info": b_info, 'Shadow_Stats': shadow_stats}

    print('nv_all_exam_data', nv_all_exam_data)
    
    return result
#read_consolidated_report('b4',True)


def read_nv_report(sheet, exam_name, df_list_of_nv):
    full_data = sheet.get_all_values()
    exam_details_list = full_data[0:9]
    exam_details = {}
    for i in range(len(exam_details_list)):
        if exam_details_list[i][0] != '':
            exam_details[exam_details_list[i]
                         [0].strip()] = exam_details_list[i][1].strip()

    if exam_details['Status'] != 'Done':
        return {'Result': 'Result Not Available', 'Exam Details': exam_details}
    data = full_data[10:]
    headers = full_data[9]
    df = pd.DataFrame(data, columns=headers)
    df['Exam Name'] = exam_name
    df['Email Id'] = df['Email Id'].str.upper()
    df_list_of_nv.append(df)

    Pass = 0  # greater than 60
    fail = 0  # less than 60 and greater than 0 or 0
    avg_pass_mark = 0
    avg_fail_mark = 0

    fail_df = df.loc[df['Is Passed'] == 'Fail']
    fail_df["Percentage"] = fail_df["Percentage"].apply(pd.to_numeric)
    avg_fail_mark = fail_df['Percentage'].mean()
    fail = len(fail_df.index)
    df = df[df['Is Passed'] != 1]

    Pass_df = df.loc[df['Is Passed'] == 'PASS']
    avg_pass_mark = Pass_df['Percentage'].apply(pd.to_numeric).mean()
    Pass_df['Percentage'] = Pass_df['Percentage'].apply(pd.to_numeric)
    Pass = len(Pass_df.index)
    pass_percentage = {"60-70":0, "70-80":0, "80-90":0, "90-100":0}
    fail_percentage = {"0-25":0, "25-50":0, "50-55":0, "55-59.99":0}

    if Pass_df.empty == False:

        pass_percentage = {
            "60-70": round(len(
            Pass_df.loc[(Pass_df['Percentage'].apply(pd.to_numeric) >= 60) & (Pass_df['Percentage'].apply(pd.to_numeric) < 70)].index) / (Pass_df.shape[0]) * 100, 2),
            "70-80": round(len(
            Pass_df.loc[(Pass_df['Percentage'].apply(pd.to_numeric) >= 70) & (Pass_df['Percentage'].apply(pd.to_numeric) < 80)].index) / (Pass_df.shape[0]) * 100, 2),
            "80-90": round(len(
            Pass_df.loc[(Pass_df['Percentage'].apply(pd.to_numeric) >= 80) & (Pass_df['Percentage'].apply(pd.to_numeric) < 90)].index) / (Pass_df.shape[0]) * 100, 2),
            "90-100": round(len(
            Pass_df.loc[(Pass_df['Percentage'].apply(pd.to_numeric) >= 90) & (Pass_df['Percentage'].apply(pd.to_numeric) <= 100)].index) / (Pass_df.shape[0]) * 100, 2),
        }
    
    if fail_df.empty == False:

        fail_percentage = {
            "0-25": round(len(
            fail_df.loc[(fail_df['Percentage'].apply(pd.to_numeric) >= 0) & (fail_df['Percentage'].apply(pd.to_numeric) < 25)].index) / (fail_df.shape[0]) * 100, 2),
            "25-50": round(len(
            fail_df.loc[(fail_df['Percentage'].apply(pd.to_numeric) >= 25) & (fail_df['Percentage'].apply(pd.to_numeric) < 50)].index) / (fail_df.shape[0]) * 100, 2),
            "50-55": round(len(
            fail_df.loc[(fail_df['Percentage'].apply(pd.to_numeric) >= 50) & (fail_df['Percentage'].apply(pd.to_numeric) < 55)].index) / (fail_df.shape[0]) * 100, 2),
            "55-59.99": round(len(
            fail_df.loc[(fail_df['Percentage'].apply(pd.to_numeric) >= 55) & (fail_df['Percentage'].apply(pd.to_numeric) < 60)].index) / (fail_df.shape[0]) * 100, 2),
        }

    return {
        'Result': 'Exam Done',
        'Total_Count': fail + Pass,
        'Pass': Pass,
        'Fail': fail,
        'Average_Pass_Mark': round(avg_pass_mark, 2),
        'Average_Fail_Mark': round(avg_fail_mark, 2),
        'Pass_Percentage': pass_percentage,
        'Fail_Percentage': fail_percentage,
        'Exam_Details': exam_details,
    }


# read_with_pandas()
# read_consolidated_report()

def read_sprint(file, sheet_name):
    sheet = file.worksheet(sheet_name)

    try:

        full_data = sheet.get_all_values()
        exam_details_list = full_data[0:9]
        data = full_data[10:]
        headers = full_data[9]
        df = pd.DataFrame(data, columns=headers)

        exam_details = {}
        for i in range(len(exam_details_list)):
            if exam_details_list[i][0] != '':
                exam_details[exam_details_list[i][0]] = exam_details_list[i][1]

        average_marks = df['Marks'].apply(pd.to_numeric).mean()
        df['Email Id'] = df['Email Id'].str.upper()
        # display(df)

        return {
            'status': 'success',
            "Stats":{
                "Exam_Details": exam_details,
                "Average_Marks": round(average_marks, 2)
            },
            "df": df[['Marks', 'Email Id']].copy(),
        }
    except Exception as e:
        # print(e)
        return {
            'status': 'fail',
            "Stats":{
            "Exam_Details": 'Not Available',
            "Average_Marks": 'Not Availble'},
            'Error': f'''{e}'''
        }



def read_m1_improvement_report(file):
    try:
        sheet = file.worksheet('CG_M1_Improvement')
    except:
        return {'is_Empty': True, 'df': None, 'stats': None}
    full_data = sheet.get_all_values()
    exam_details_list = full_data[0:9]
    exam_details = {}
    for i in range(len(exam_details_list)):
        if exam_details_list[i][0] != '':
            exam_details[exam_details_list[i][0].strip()] = exam_details_list[i][1].strip()
    if exam_details['Status'] != 'Done':
        return {'is_Empty': True, 'stats':{'Exam Details': exam_details}}
    data = full_data[10:]
    headers = full_data[9]
    df = pd.DataFrame(data, columns=headers)

    imprv_fail_df = df.loc[df['Improvement_counter'] == '2']
    imprv_fail_df["Improvement_Total"] = imprv_fail_df["Improvement_Total"].apply(pd.to_numeric)
    avg_fail_mark = imprv_fail_df['Improvement_Total'].mean()
    fail = len(imprv_fail_df.index)
    df = df[df.Improvement_counter != 1]

    imprv_pass_df = df.loc[df['Improvement_counter'] == '1']
    avg_pass_mark = imprv_pass_df['Improvement_Total'].apply(pd.to_numeric).mean()
    imprv_pass_df['Improvement_Total'] = imprv_pass_df['Improvement_Total'].apply(pd.to_numeric)
    Pass = len(imprv_pass_df.index)

    pass_percentage = {"60-70":0, "70-80":0, "80-90":0, "90-100":0}
    fail_percentage = {"0-25":0, "25-50":0, "50-55":0, "55-59.99":0}

    if imprv_pass_df.empty == False:

        pass_percentage['60-70'] = round(len(
            imprv_pass_df.loc[(imprv_pass_df['Improvement_Total'].apply(pd.to_numeric) >= 60) & (imprv_pass_df['Improvement_Total'].apply(pd.to_numeric) < 70)].index) / (imprv_pass_df.shape[0]) * 100, 2)
        pass_percentage['70-80'] = round(len(
            imprv_pass_df.loc[(imprv_pass_df['Improvement_Total'].apply(pd.to_numeric) >= 70) & (imprv_pass_df['Improvement_Total'].apply(pd.to_numeric) < 80)].index) / (imprv_pass_df.shape[0]) * 100, 2)
        pass_percentage['80-90'] = round(len(
            imprv_pass_df.loc[(imprv_pass_df['Improvement_Total'].apply(pd.to_numeric) >= 80) & (imprv_pass_df['Improvement_Total'].apply(pd.to_numeric) < 90)].index) / (imprv_pass_df.shape[0]) * 100, 2)
        pass_percentage['90-100'] = round(len(
            imprv_pass_df.loc[(imprv_pass_df['Improvement_Total'].apply(pd.to_numeric) >= 90) & (imprv_pass_df['Improvement_Total'].apply(pd.to_numeric) <= 100)].index) / (imprv_pass_df.shape[0]) * 100, 2)

    if imprv_fail_df.empty == False:

        fail_percentage = {
            "0-25": round(len(
            imprv_fail_df.loc[(imprv_fail_df['Improvement_Total'].apply(pd.to_numeric) >= 0) & (imprv_fail_df['Improvement_Total'].apply(pd.to_numeric) < 25)].index) / (imprv_fail_df.shape[0]) * 100, 2),
            "25-50": round(len(
            imprv_fail_df.loc[(imprv_fail_df['Improvement_Total'].apply(pd.to_numeric) >= 25) & (imprv_fail_df['Improvement_Total'].apply(pd.to_numeric) < 50)].index) / (imprv_fail_df.shape[0]) * 100, 2),
            "50-55": round(len(
            imprv_fail_df.loc[(imprv_fail_df['Improvement_Total'].apply(pd.to_numeric) >= 50) & (imprv_fail_df['Improvement_Total'].apply(pd.to_numeric) < 55)].index) / (imprv_fail_df.shape[0]) * 100, 2),
            "55-59.99": round(len(
            imprv_fail_df.loc[(imprv_fail_df['Improvement_Total'].apply(pd.to_numeric) >= 55) & (imprv_fail_df['Improvement_Total'].apply(pd.to_numeric) < 60)].index) / (imprv_fail_df.shape[0]) * 100, 2)
            
        }
        
    df = df[['Capgemini Email ID', 'Improvement_Total']].copy()
    
    df = df.rename(
        columns={'Capgemini Email ID': 'Email Id'})
    df['Email Id'] = df["Email Id"].str.upper()
    return {
        'stats':{
        "Exam_Details": exam_details,
        "Average_Pass_Marks": round(avg_pass_mark, 2),
        "Average_Fail_Marks": round(avg_fail_mark, 2),
        "Pass": Pass,
        "Fail": fail,
        "Pass_Percentage": pass_percentage,
        "Fail_Percentage": fail_percentage
        },
        "df": df,
        'is_Empty': False
    }



def read_shadow_project(file):
    try:
        sheet = file.worksheet('Shadow_Project')
    except:
        return {'is_Empty': True, 'df': None, 'stats': None}
    full_data = sheet.get_all_values()
    exam_details_list = full_data[0:9]
    exam_details = {}
    for i in range(len(exam_details_list)):
        if exam_details_list[i][0] != '':
            exam_details[exam_details_list[i][0].strip()] = exam_details_list[i][1].strip()
    if exam_details['Status'] != 'Done':
        return {'is_Empty': True, 'stats':{'Exam Details': exam_details}, 'df': None}

    data = full_data[10:]
    headers = full_data[9]
    df = pd.DataFrame(data, columns=headers)
    # display(df)
    df = df[['Marks', 'Email Id']].copy()
    df = df.rename({'Marks': 'Shadow_Project'}, axis=1)



    return {
        'is_Empty': False,
        'stats':{
            'Exam_Details': exam_details, 
            'Absent': len(df.loc[df['Shadow_Project'] == 'AB'].index),
            'Drop_Out': len(df.loc[df['Shadow_Project'] == 'DO'].index),
            'Total': df.shape[0]
        },
        'df': df
    }






#print(json.dumps(read_consolidated_report(input('Enter Name: ')), indent=4))
# print(read_consolidated_report('b4'))
