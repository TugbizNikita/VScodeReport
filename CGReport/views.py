from django.http import JsonResponse, HttpResponse
from .sheets import read_consolidated_report, read_wpa_report
from .sheets2 import read_batch_lsr, read_lsr, read_wpr, read_batch_consolidated, read_candidates
from .attendance import read_batch_attendance
from io import BytesIO
import pandas as pd
from datetime import datetime
from requests.exceptions import HTTPError
import pandera as pa

def index(request):
    file_name = request.GET.get('file-name')
    return JsonResponse(read_consolidated_report(file_name, False))

def wpr_report(request):
    file_name = request.GET.get('file-name')
    return JsonResponse(read_wpa_report(file_name, False))

def download_consolidated_report(request):
    # file_name = request.GET.get('file-name')
    file_names = request.GET.get('file-names').split(',')
    print(file_names)
    # df = df.rename({'NV1': 'W1_MCQ_1'}, axis=1)
    with BytesIO() as b:
        # Use the StringIO object as the filehandle.
        writer = pd.ExcelWriter(b, engine='xlsxwriter')

        for i in file_names:
            cons_report = read_consolidated_report(i, True)
            df = cons_report['df']
            df.to_excel(writer, sheet_name=cons_report['sheet_name'], index=False)
            #time.sleep(30)

        writer.save()
        datestring = datetime.strftime(datetime.now(), ' %d-%m-%Y %H:%M:%S')
        # Set up the Http response.
        filename = 'Consolidated'+datestring+'.xlsx'
        response = HttpResponse(
            b.getvalue(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response

def download_wpr_report(request):
    file_names = request.GET.get('file-names').split(',')
    print(file_names)
    with BytesIO() as b:
        # Use the StringIO object as the filehandle.
        writer = pd.ExcelWriter(b, engine='xlsxwriter')

        for i in file_names:
            wpr_report = read_wpa_report(i, True)
            df = wpr_report['wpr_data_df']
            test = "test"
            df.to_excel(writer, sheet_name=wpr_report['sheet_name'], index=False)
            #time.sleep(30)

        writer.save()
        datestring = datetime.strftime(datetime.now(), ' %d-%m-%Y %H:%M:%S')
        
        # Set up the Http response.
        filename = 'Weekly Performance Report'+datestring+'.xlsx'
        response = HttpResponse(
            b.getvalue(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response

def wsr_report(request):
    file_name = request.GET.get('file-name')
    return JsonResponse(read_batch_lsr(file_name, False))

def download_wsr(request):
    # file_name = request.GET.get('file-name')
    file_names = request.GET.get('file-names').split(',')
    print(file_names)
    # df = df.rename({'NV1': 'W1_MCQ_1'}, axis=1)
    with BytesIO() as b:
        # Use the StringIO object as the filehandle.
        writer = pd.ExcelWriter(b, engine='xlsxwriter')

        for i in file_names:
            wsr_report = read_batch_lsr(i, True)
            df = wsr_report['lsr_data_df']
            
            #df.to_excel(writer, sheet_name="wsr")
            df.to_excel(writer, sheet_name=wsr_report['sheet_name'], index=False)
            #time.sleep(30)

        writer.save()
        datestring = datetime.strftime(datetime.now(), ' %d-%m-%Y %H:%M:%S')

        # Set up the Http response.
        filename = 'Weekly Summary Report'+datestring+'.xlsx'
        response = HttpResponse(
            b.getvalue(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response

def download_attendance(request):
    # file_name = request.GET.get('file-name')
    file_names = request.GET.get('file-names').split(',')
    print(file_names)
    # df = df.rename({'NV1': 'W1_MCQ_1'}, axis=1)
    with BytesIO() as b:
        # Use the StringIO object as the filehandle.
        writer = pd.ExcelWriter(b, engine='xlsxwriter')

        for i in file_names:
            attendance_report = read_batch_attendance(i, True)
            df = attendance_report['attendance_date_df']
            
            df.to_excel(writer, sheet_name=attendance_report['sheet_name'], index=False)
            #time.sleep(30)

        writer.save()
        # Set up the Http response.
        filename = 'Attendance.xlsx'
        response = HttpResponse(
            b.getvalue(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response

def validate_wsr_working(request):
    myExpMsg = ""
    # file_name = request.GET.get('file-name')
    file_names = request.GET.get('file-names').split(',')
    print(file_names)
    # df = df.rename({'NV1': 'W1_MCQ_1'}, axis=1)
    with BytesIO() as b:
        # Use the StringIO object as the filehandle.
        writer = pd.ExcelWriter(b, engine='xlsxwriter')

        for i in file_names:
            wsr_report = read_batch_lsr(i, True)
            df = wsr_report['lsr_data_df']
            print("lsr_data_df1", df.columns)
            col_headers = df.columns
            
            config_headers = ['Vendor', 'Sr.No', 'LOT', 'Variant', 'Batch Name', 'CFMG Batch Code','Batch Type', 'Location', 'Start Date', 'End Date',
                        'Initial Batch Size', 'Dropout/Abscondee Count', 'Transfer-Out Count',
                        'Transfer-In Count', 'Current Batch Size', 'Batch Mentor',
                        'Learning status', 'Above Average Pax Count', 'Average Pax Count',
                        'Below Average Pax Count', 'DO', 'NA']

            diff_col = set(config_headers) - set(col_headers)
            diff_col_list = list(diff_col)
            if(len(diff_col_list) > 0):
                return JsonResponse({'Column name missing': diff_col_list})
            
            try:
                schema = pa.DataFrameSchema({
                "Sr.No": pa.Column(int, checks=pa.Check.le(10)),
                "Batch Mentor": pa.Column(str),
                "Learning status": pa.Column(str),
                })
                validated_df = schema(df)
            except Exception as e:
                myExpMsg = "the error is " + str(e)
                return JsonResponse({'error 3': myExpMsg})

            #df.to_excel(writer, sheet_name="wsr")
            df.to_excel(writer, sheet_name=wsr_report['sheet_name'], index=False)
            #time.sleep(30)

        writer.save()
        datestring = datetime.strftime(datetime.now(), ' %d-%m-%Y-%H-%M-%S')

        # Set up the Http response.
        filename = 'Weekly Summary Report'+datestring+'.xlsx'
        response = HttpResponse(
            b.getvalue(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response['Content-Disposition'] = 'attachment; filename=%s' % filename
        return response


def validate_wsr(request):
    # file_name = request.GET.get('file-name')
    file_names = request.GET.get('file-names').split(',')
    print(file_names)
    # df = df.rename({'NV1': 'W1_MCQ_1'}, axis=1)
    with BytesIO() as b:
        # Use the StringIO object as the filehandle.
        writer = pd.ExcelWriter(b, engine='xlsxwriter')

        for i in file_names:
            wsr_report = read_lsr(i)
            df = wsr_report
            col_headers = df.columns
            
            config_headers = ['Week_No', 'Batch Mentor', 'Learning status']

            diff_col = set(config_headers) - set(col_headers)
            diff_col_list = list(diff_col)
            if(len(diff_col_list) > 0):
                return JsonResponse({'Column name missing': diff_col_list})
            else:
                return JsonResponse({'Sucess':'Good to go'})
            
        response = HttpResponse(
            b.getvalue()
        )
        
        return response

def validate_wpr(request):
    # file_name = request.GET.get('file-name')
    file_names = request.GET.get('file-names').split(',')
    print(file_names)
    # df = df.rename({'NV1': 'W1_MCQ_1'}, axis=1)
    with BytesIO() as b:
        # Use the StringIO object as the filehandle.
        writer = pd.ExcelWriter(b, engine='xlsxwriter')

        for i in file_names:
            wpr_report = read_wpr(i)
            df = wpr_report
            col_headers = df.columns
            
            config_headers = ['Sr. No.', 'Vendor', 'CFMG Batch Code', 'Batch Name', 'Employee ID', 'Employee Full Name', 
                              'W1-Technical', 'W1-Soft Skill', 'W1-Learning Status Remark', 'W2-Technical', 'W2-Soft Skill', 'W2-Learning Status Remark',
                              'W3-Technical', 'W3-Soft Skill', 'W3-Learning Status Remark', 'W4-Technical', 'W4-Soft Skill', 'W4-Learning Status Remark',
                              'W5-Technical', 'W5-Soft Skill', 'W5-Learning Status Remark', 'W6-Technical',	'W6-Soft Skill', 'W6-Learning Status Remark',
                              'W7-Technical', 'W7-Soft Skill', 'W7-Learning Status Remark',	'W8-Technical',	'W8-Soft Skill', 'W8-Learning Status Remark',
                              'W9-Technical', 'W9-Soft Skill', 'W9-Learning Status Remark',	'W10-Technical', 'W10-Soft Skill', 'W10-Learning Status Remark']

            diff_col = set(config_headers) - set(col_headers)
            diff_col_list = list(diff_col)
            if(len(diff_col_list) > 0):
                return JsonResponse({'Column name missing': diff_col_list})
            else:
                return JsonResponse({'Sucess':'Good to go'})
            
        response = HttpResponse(
            b.getvalue()
        )
        
        return response

def validate_wpr(request):
    # file_name = request.GET.get('file-name')
    file_names = request.GET.get('file-names').split(',')
    print(file_names)
    # df = df.rename({'NV1': 'W1_MCQ_1'}, axis=1)
    with BytesIO() as b:
        # Use the StringIO object as the filehandle.
        writer = pd.ExcelWriter(b, engine='xlsxwriter')

        for i in file_names:
            wpr_report = read_wpr(i)
            df = wpr_report
            col_headers = df.columns
            
            config_headers = ['Sr. No.', 'Vendor', 'CFMG Batch Code', 'Batch Name', 'Employee ID', 'Employee Full Name', 
                              'W1-Technical', 'W1-Soft Skill', 'W1-Learning Status Remark', 'W2-Technical', 'W2-Soft Skill', 'W2-Learning Status Remark',
                              'W3-Technical', 'W3-Soft Skill', 'W3-Learning Status Remark', 'W4-Technical', 'W4-Soft Skill', 'W4-Learning Status Remark',
                              'W5-Technical', 'W5-Soft Skill', 'W5-Learning Status Remark', 'W6-Technical',	'W6-Soft Skill', 'W6-Learning Status Remark',
                              'W7-Technical', 'W7-Soft Skill', 'W7-Learning Status Remark',	'W8-Technical',	'W8-Soft Skill', 'W8-Learning Status Remark',
                              'W9-Technical', 'W9-Soft Skill', 'W9-Learning Status Remark',	'W10-Technical', 'W10-Soft Skill', 'W10-Learning Status Remark']

            diff_col = set(config_headers) - set(col_headers)
            diff_col_list = list(diff_col)
            if(len(diff_col_list) > 0):
                return JsonResponse({'Column name missing': diff_col_list})
            else:
                return JsonResponse({'Sucess':'Good to go'})
            
        response = HttpResponse(
            b.getvalue()
        )
        
        return response
    
def validate_consolidated(request):
    # file_name = request.GET.get('file-name')
    file_names = request.GET.get('file-names').split(',')
    print(file_names)
    # df = df.rename({'NV1': 'W1_MCQ_1'}, axis=1)
    with BytesIO() as b:
        # Use the StringIO object as the filehandle.
        writer = pd.ExcelWriter(b, engine='xlsxwriter')

        for i in file_names:
            consolidated_report = read_batch_consolidated(i)
            df = consolidated_report
            col_headers = df.columns
            
            config_headers = ['Vendor', 'LOT','Variant', 'Batch Name', 'CFMG Code', 'Type', 'Location', 'Start Date', 'End Date', 'Intial Size', 'Total']

            diff_col = set(config_headers) - set(col_headers)
            diff_col_list = list(diff_col)
            if(len(diff_col_list) > 0):
                return JsonResponse({'Column name missing': diff_col_list})
            else:
                return JsonResponse({'Sucess':'Good to go'})
            
        response = HttpResponse(
            b.getvalue()
        )
        
        return response

def validate_candidates(request):
    # file_name = request.GET.get('file-name')
    file_names = request.GET.get('file-names').split(',')
    print(file_names)
    # df = df.rename({'NV1': 'W1_MCQ_1'}, axis=1)
    with BytesIO() as b:
        # Use the StringIO object as the filehandle.
        writer = pd.ExcelWriter(b, engine='xlsxwriter')

        for i in file_names:
            candidate_df = read_candidates(i)
            col_headers = candidate_df.columns
            
            config_headers = ['Sr No.', 'Candidate name', 'Phone No.', 'Personal Email ID', 'Superset ID', 'College name', 'Branch', 'Skills',
                            	'Training Start Date', 'Batch Code', 'Week_No',	'Drop_Out', 'Drop_Out_Date', 'Transfer_In',	'Transfer_Out',	'CR']

            diff_col = set(config_headers) - set(col_headers)
            diff_col_list = list(diff_col)
            if(len(diff_col_list) > 0):
                return JsonResponse({'Column name missing': diff_col_list})
            else:
                return JsonResponse({'Sucess':'Good to go'})
            
        response = HttpResponse(
            b.getvalue()
        )
        
        return response


