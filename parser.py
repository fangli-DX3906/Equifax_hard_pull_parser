# This code parses the Equifax hard pull data into structured tables and push them to the Google BigQuery
# Created by Fang Li on Sept 27, 2024

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from google.cloud import bigquery
import re
import time
# py3.7, no Literal in typing but in typing extension
# from typing import Literal 


# global setting
job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
)

# Parser class
class FFFParser:
    def __init__(self, year: int, month: int, project_id: str, dataset_id: str):
        self.year = year
        self.month = month
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.fff_name = 'insert hard pull dress here'
        self.bq_prefix = f'{self.project_id}.{self.dataset_id}'
        self.client = bigquery.Client(project=project_id)
        self.seg_names = ['CA', 'FA', 'F2', 'AK', 'FN', 'DT', 'ES', 'EF', 'E2', 'OI', 'BP', 'CO', 'FM', 'LI', 'FO', 
                          'NR', 'MI', 'TL', 'FC', 'GN', 'TC', 'NT', 'CS', 'FB', 'FI', 'LO', 'IQ', 'CD', 'BS']
        self.table_names = self.seg_names + ['header']
        self.header_cols_dict = {
            "Report_Type": (0, 4),
            "Customer_Reference_Number": (5, 17), 
            "Member_Number": (18, 28),
            "Consumer_Referral_Number": (29, 32),
            "ECOA_Inquiry_Type": (34, 35),
            "Output_Format_Code": (36, 37),
            "Hit_No_Hit_Designator": (41, 42), 
            "File_Since_Date": (43, 53), 
            "Date_of_Last_Activity": (54, 64),
            "Date_of_This_Report": (65, 75),
            "Subjects_Last_Name": (80, 105),
            "Subjects_First_Name": (106, 121),
            "Middle_Name_or_Initial": (122, 137),
            "Suffix": (138, 140),
            "Spouses_Name": (141, 156),
            "Record_Code_SS": (160, 162),
            "Subjects_Social_Insurance_Number": (162, 171),
            "Subjects_Date_of_Birth_Age": (172, 182), 
            "Record_Code_SO": (190, 192), 
            "Total_Number_of_Inquiries": (202, 205),
            "Warning_Message": (208, 209),
            "Alert_Indicator_Flag": (210, 211), 
            "Segment_Counter": (240, 302), 
            "Alert_Flag": (312, 314), 
            "Deposit_Flag": (315, 316), 
            "SAFESCAN_Byte_1": (317, 318),
            "SAFESCAN_ID_Byte_2": (318, 319)
        }
        
    def _construct_fetch_query(self, how: str, who: str) -> str:
        if how not in ['fetch', 'push']:
            raise ValueError
        if who not in self.table_names and who != '':
            raise ValueError
        if how == 'fetch':
            fetch_query = f"""
                SELECT * FROM `{self.fff_name}`
                WHERE file_date >= DATE(SAFE_CAST({self.year} AS INT64), SAFE_CAST({self.month} AS INT64), 1) AND
                file_date <= LAST_DAY(DATE(SAFE_CAST({self.year} AS INT64), SAFE_CAST({self.month} AS INT64), 1))
            """
        elif how == 'push':
            which_table = f'fff_{who}_parsed'
            fetch_query = f"""
                SELECT * FROM `{self.bq_prefix}.{which_table}`
            """ 
        return fetch_query

    def fetch_date_from_google_bigquery(self):
        fetch_query = self._construct_fetch_query(how='fetch', who='')
        try:
            query_job = self.client.query(fetch_query)
        except RefreshError:
            print('Reauthentication is needed. Please run `gcloud auth application-default login` to reauthenticate.')
            
        fetch_job = query_job.result()
        self.raw_data = fetch_job.to_dataframe()
        self.data = self.raw_data[['id', 'file_name', 'file_date', 'business_partner_id', 'file_raw_content']].copy()
        self.data = self.data[~self.data['file_raw_content'].isna()]
        for col in self.header_cols_dict.keys():
            self.data[col] = None
        for seg in self.seg_names:
            self.data[f'{seg}_num_records'] = -1
            self.data[f'{seg}_ref_flag'] = ''
        print(f'******************** {self.year}.{self.month} FFF data has been retrieved ! ********************')
        
    def push_table_to_google_bigquery(self):
        self._parse_header()
        
        # push segment tables         
        for seg in self.seg_names:
            exec(f'self._parse_{seg}()')
            time.sleep(1)
            print(f'{seg} info has been pushed to BigQuery')
            
        # push the header table
        self.data.drop(columns='mfile', inplace=True)
        self.data.reset_index(drop=True)
        push_job = self.client.load_table_from_dataframe(self.data, f'{self.bq_prefix}.fff_header_parsed')
        push_job.result()
        print('Header info table has been pushed to BigQuery')
        print('******************** Push complete ! ********************')
        
    def _parse_header(self):
        self.data['mfile'] = self.data['file_raw_content'].apply(lambda x: x[x.index('FULL'):])
        for key, val in self.header_cols_dict.items():
            self.data[key] = self.data.mfile.apply(lambda x: x[val[0]:val[1]])
       
    # seg1: parsing CA     
    def _parse_CA(self):
        self.CA_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 
                     'Street_Number', 'Street_Name_Direction_Apartment',
                     'City', 'Province', 'Postal_Code', 'Residence_Since', 'Indicator_Code']
        )
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("CA ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'CA_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'CA{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.CA_table.loc[count, 'match_flag'] = fg
                    self.CA_table.loc[count, 'bus_ptnr'] = bp
                    self.CA_table.loc[count, 'file_date'] = dt
                    self.CA_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.CA_table.loc[count, 'Street_Number'] = segment[3:13]
                    self.CA_table.loc[count, 'Street_Name_Direction_Apartment'] = segment[14:40]
                    self.CA_table.loc[count, 'City'] = segment[80:100]
                    self.CA_table.loc[count, 'Province'] = segment[101:103]
                    self.CA_table.loc[count, 'Postal_Code'] = segment[104:110]
                    self.CA_table.loc[count, 'Residence_Since'] = segment[111:118]
                    self.CA_table.loc[count, 'Indicator_Code'] = segment[118:119]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'CA_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.CA_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.CA_table, f'{self.bq_prefix}.fff_CA_parsed')
            push_job.result()          
      
    # seg2: parsing FA
    def _parse_FA(self):
        self.FA_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 
                     'Street_Number', 'Street_Name_Direction_Apartment',
                     'City', 'Province', 'Postal_Code', 'Residence_Since', 'Indicator_Code']
        )       
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("FA ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'FA_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'FA{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.FA_table.loc[count, 'match_flag'] = fg
                    self.FA_table.loc[count, 'bus_ptnr'] = bp
                    self.FA_table.loc[count, 'file_date'] = dt
                    self.FA_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.FA_table.loc[count, 'Street_Number'] = segment[3:13]
                    self.FA_table.loc[count, 'Street_Name_Direction_Apartment'] = segment[14:40]
                    self.FA_table.loc[count, 'City'] = segment[80:100]
                    self.FA_table.loc[count, 'Province'] = segment[101:103]
                    self.FA_table.loc[count, 'Postal_Code'] = segment[104:110]
                    self.FA_table.loc[count, 'Residence_Since'] = segment[111:118]
                    self.FA_table.loc[count, 'Indicator_Code'] = segment[118:119]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'FA_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.FA_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.FA_table, f'{self.bq_prefix}.fff_FA_parsed')
            push_job.result() 
    
    # seg3: parsing F2
    def _parse_F2(self):
        self.F2_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 
                     'Street_Number', 'Street_Name_Direction_Apartment',
                     'City', 'Province', 'Postal_Code', 'Residence_Since', 'Indicator_Code']
        )       
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("F2 ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            self.data.loc[ncol, 'F2_num_records'] = n_record
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'F2{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.F2_table.loc[count, 'match_flag'] = fg
                    self.F2_table.loc[count, 'bus_ptnr'] = bp
                    self.F2_table.loc[count, 'file_date'] = dt
                    self.F2_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.F2_table.loc[count, 'Street_Number'] = segment[3:13]
                    self.F2_table.loc[count, 'Street_Name_Direction_Apartment'] = segment[14:40]
                    self.F2_table.loc[count, 'City'] = segment[80:100]
                    self.F2_table.loc[count, 'Province'] = segment[101:103]
                    self.F2_table.loc[count, 'Postal_Code'] = segment[104:110]
                    self.F2_table.loc[count, 'Residence_Since'] = segment[111:118]
                    self.F2_table.loc[count, 'Indicator_Code'] = segment[118:119]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'F2_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.F2_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.F2_table, f'{self.bq_prefix}.fff_F2_parsed')
            push_job.result() 
    
    # seg4: parsing AK
    def _parse_AK(self):
        self.AK_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Street_Number', 'First_Name', 
                     'Middle_Name_Initial', 'Suffix', 'Spouse_Name', 'Lagel_Name_Change']
        )    
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("AK ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'AK_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'AK{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.AK_table.loc[count, 'match_flag'] = fg
                    self.AK_table.loc[count, 'bus_ptnr'] = bp
                    self.AK_table.loc[count, 'file_date'] = dt
                    self.AK_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.AK_table.loc[count, 'Street_Number'] = segment[3:28]
                    self.AK_table.loc[count, 'First_Name'] = segment[29:44]
                    self.AK_table.loc[count, 'Middle_Name_Initial'] = segment[45:60]
                    self.AK_table.loc[count, 'Suffix'] = segment[61:63]
                    self.AK_table.loc[count, 'Spouse_Name'] = segment[80:95]
                    self.AK_table.loc[count, 'Lagel_Name_Change'] = segment[96:97]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'AK_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.AK_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.AK_table, f'{self.bq_prefix}.fff_AK_parsed')
            push_job.result() 
       
    # seg5: parsing FN
    def _parse_FN(self):
        self.FN_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Street_Number', 'First_Name', 
                     'Middle_Name_Initial', 'Suffix', 'Spouse_Name', 'Lagel_Name_Change']
        )
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("FN ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'FN_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'FN{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.FN_table.loc[count, 'match_flag'] = fg
                    self.FN_table.loc[count, 'bus_ptnr'] = bp
                    self.FN_table.loc[count, 'file_date'] = dt
                    self.FN_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.FN_table.loc[count, 'Street_Number'] = segment[3:28]
                    self.FN_table.loc[count, 'First_Name'] = segment[29:44]
                    self.FN_table.loc[count, 'Middle_Name_Initial'] = segment[45:60]
                    self.FN_table.loc[count, 'Suffix'] = segment[61:63]
                    self.FN_table.loc[count, 'Spouse_Name'] = segment[80:95]
                    self.FN_table.loc[count, 'Lagel_Name_Change'] = segment[96:97]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'FN_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.FN_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.FN_table, f'{self.bq_prefix}.fff_FN_parsed')
            push_job.result() 
      
    # seg6: parsing DT
    def _parse_DT(self):
        self.DT_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Date_Subject_Death']
        )       
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("DT ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'DT_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'DT{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.DT_table.loc[count, 'match_flag'] = fg
                    self.DT_table.loc[count, 'bus_ptnr'] = bp
                    self.DT_table.loc[count, 'file_date'] = dt
                    self.DT_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.DT_table.loc[count, 'Date_Subject_Death'] = segment[3:10]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'DT_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.DT_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.DT_table, f'{self.bq_prefix}.fff_DT_parsed')
            push_job.result() 
    
    # seg7: parsing ES
    def _parse_ES(self):
        self.ES_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Occupation', 'Employer', 
                     'City_Employment', 'Province_Employment', 'Date_Employed',
                     'Date_Verified', 'Verification_Status', 'Monthly_Salary_Indicator', 'Date_Left']
        )    
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("ES ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'ES_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'ES{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.ES_table.loc[count, 'match_flag'] = fg
                    self.ES_table.loc[count, 'bus_ptnr'] = bp
                    self.ES_table.loc[count, 'file_date'] = dt
                    self.ES_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.ES_table.loc[count, 'Occupation'] = segment[3:37]
                    self.ES_table.loc[count, 'Employer'] = segment[38:72]
                    self.ES_table.loc[count, 'City_Employment'] = segment[80:88]
                    self.ES_table.loc[count, 'Province_Employment'] = segment[89:91]
                    self.ES_table.loc[count, 'Date_Employed'] = segment[92:99]
                    self.ES_table.loc[count, 'Date_Verified'] = segment[100:107]
                    self.ES_table.loc[count, 'Verification_Status'] = segment[108:109]
                    self.ES_table.loc[count, 'Monthly_Salary_Indicator'] = segment[110:118]
                    self.ES_table.loc[count, 'Date_Left'] = segment[119:126]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'ES_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.ES_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.ES_table, f'{self.bq_prefix}.fff_ES_parsed')
            push_job.result()
        
    # seg8: parsing EF    
    def _parse_EF(self):
        self.EF_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Occupation', 'Employer', 
                     'City_Employment', 'Province_Employment', 'Date_Employed',
                     'Date_Verified', 'Verification_Status', 'Monthly_Salary_Indicator', 'Date_Left']
        )     
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("EF ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'EF_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'EF{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.EF_table.loc[count, 'match_flag'] = fg
                    self.EF_table.loc[count, 'bus_ptnr'] = bp
                    self.EF_table.loc[count, 'file_date'] = dt
                    self.EF_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.EF_table.loc[count, 'Occupation'] = segment[3:37]
                    self.EF_table.loc[count, 'Employer'] = segment[38:72]
                    self.EF_table.loc[count, 'City_Employment'] = segment[80:88]
                    self.EF_table.loc[count, 'Province_Employment'] = segment[89:91]
                    self.EF_table.loc[count, 'Date_Employed'] = segment[92:99]
                    self.EF_table.loc[count, 'Date_Verified'] = segment[100:107]
                    self.EF_table.loc[count, 'Verification_Status'] = segment[108:109]
                    self.EF_table.loc[count, 'Monthly_Salary_Indicator'] = segment[110:118]
                    self.EF_table.loc[count, 'Date_Left'] = segment[119:126]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'EF_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.EF_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.EF_table, f'{self.bq_prefix}.fff_EF_parsed')
            push_job.result()
        
    # seg9: parsing E2    
    def _parse_E2(self):
        self.E2_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Occupation', 'Employer', 
                     'City_Employment', 'Province_Employment', 'Date_Employed',
                     'Date_Verified', 'Verification_Status', 'Monthly_Salary_Indicator', 'Date_Left']
        )
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("E2 ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'E2_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'E2{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.E2_table.loc[count, 'match_flag'] = fg
                    self.E2_table.loc[count, 'bus_ptnr'] = bp
                    self.E2_table.loc[count, 'file_date'] = dt
                    self.E2_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.E2_table.loc[count, 'Occupation'] = segment[3:37]
                    self.E2_table.loc[count, 'Employer'] = segment[38:72]
                    self.E2_table.loc[count, 'City_Employment'] = segment[80:88]
                    self.E2_table.loc[count, 'Province_Employment'] = segment[89:91]
                    self.E2_table.loc[count, 'Date_Employed'] = segment[92:99]
                    self.E2_table.loc[count, 'Date_Verified'] = segment[100:107]
                    self.E2_table.loc[count, 'Verification_Status'] = segment[108:109]
                    self.E2_table.loc[count, 'Monthly_Salary_Indicator'] = segment[110:118]
                    self.E2_table.loc[count, 'Date_Left'] = segment[119:126]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'E2_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.E2_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.E2_table, f'{self.bq_prefix}.fff_E2_parsed')
            push_job.result()
        
    # seg12: parsing OI    
    def _parse_OI(self):
        self.OI_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Date_Reported', 
                     'Income_Amount_Blank_Given_Amount_Unavaiable', 
                     'Income_Source', 'Date_Verified', 'Verification_Status']
        )        
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("OI ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'OI_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'OI{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.OI_table.loc[count, 'match_flag'] = fg
                    self.OI_table.loc[count, 'bus_ptnr'] = bp
                    self.OI_table.loc[count, 'file_date'] = dt
                    self.OI_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.OI_table.loc[count, 'Date_Reported'] = segment[3:10]
                    self.OI_table.loc[count, 'Income_Amount_Blank_Given_Amount_Unavaiable'] = segment[11:17]
                    self.OI_table.loc[count, 'Income_Source'] = segment[18:58]
                    self.OI_table.loc[count, 'Date_Verified'] = segment[59:66]
                    self.OI_table.loc[count, 'Verification_Status'] = segment[67:68]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'OI_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.OI_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.OI_table, f'{self.bq_prefix}.fff_OI_parsed')
            push_job.result()
     
    # seg13: parsing BP
    def _parse_BP(self):
        self.BP_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Foreign_Bureau_Code', 
                     'Date_Filed', 'Name_Court', 'Court_Number', 'Type_Bankruptcy', 'How_Filed',
                     'Deposition_Codes', 'Amount_Liability', 'Asset_Amount', 'Date_Settled',
                     'Narrative_Code_1', 'Narrative_Code_2', 'Case_Number']
        )        
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("BP ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'BP_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'BP{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.BP_table.loc[count, 'match_flag'] = fg
                    self.BP_table.loc[count, 'bus_ptnr'] = bp
                    self.BP_table.loc[count, 'file_date'] = dt
                    self.BP_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.BP_table.loc[count, 'Foreign_Bureau_Code'] = segment[3:4]
                    self.BP_table.loc[count, 'Date_Filed'] = segment[5:12]
                    self.BP_table.loc[count, 'Name_Court'] = segment[13:33]
                    self.BP_table.loc[count, 'Court_Number'] = segment[52:62]
                    self.BP_table.loc[count, 'Type_Bankruptcy'] = segment[63:64]
                    self.BP_table.loc[count, 'How_Filed'] = segment[65:66]
                    self.BP_table.loc[count, 'Deposition_Codes'] = segment[67:68]
                    self.BP_table.loc[count, 'Amount_Liability'] = segment[69:75]
                    self.BP_table.loc[count, 'Asset_Amount'] = segment[80:86]
                    self.BP_table.loc[count, 'Date_Settled'] = segment[87:94]
                    self.BP_table.loc[count, 'Narrative_Code_1'] = segment[95:97]
                    self.BP_table.loc[count, 'Narrative_Code_2'] = segment[98:100]
                    self.BP_table.loc[count, 'Case_Number'] = segment[101:143]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'BP_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.BP_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.BP_table, f'{self.bq_prefix}.fff_BP_parsed')
            push_job.result()
        
    # seg14: parsing CO    
    def _parse_CO(self):
        self.CO_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Foreign_Bureau_Code', 
                     'Date_Reported', 'Name_Member', 'Member_Number', 'Amount', 'Balance', 'Type', 
                     'Narrative_Code_1', 'Narrative_Code_2', 'Industry_Code', 'Reason_Code', 'Date_Paid', 
                     'Date_Last_Payment', 'Creditors_Account_Number_Name', 'Ledger_Name']
        )       
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("CO ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'CO_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'CO{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.CO_table.loc[count, 'match_flag'] = fg
                    self.CO_table.loc[count, 'bus_ptnr'] = bp
                    self.CO_table.loc[count, 'file_date'] = dt
                    self.CO_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.CO_table.loc[count, 'Foreign_Bureau_Code'] = segment[3:4]
                    self.CO_table.loc[count, 'Date_Reported'] = segment[5:12]
                    self.CO_table.loc[count, 'Name_Member'] = segment[13:33]
                    self.CO_table.loc[count, 'Member_Number'] = segment[52:62]
                    self.CO_table.loc[count, 'Amount'] = segment[63:69]
                    self.CO_table.loc[count, 'Balance'] = segment[70:76]
                    self.CO_table.loc[count, 'Type'] = segment[77:78]
                    self.CO_table.loc[count, 'Narrative_Code_1'] = segment[80:82]
                    self.CO_table.loc[count, 'Narrative_Code_2'] = segment[83:85]
                    self.CO_table.loc[count, 'Industry_Code'] = segment[86:88]
                    self.CO_table.loc[count, 'Reason_Code'] = segment[89:90]
                    self.CO_table.loc[count, 'Date_Paid'] = segment[91:98]
                    self.CO_table.loc[count, 'Date_Last_Payment'] = segment[99:106]
                    self.CO_table.loc[count, 'Creditors_Account_Number_Name'] = segment[107:157]
                    self.CO_table.loc[count, 'Ledger_Name'] = segment[160:177]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'CO_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)

        if count > 0:
            self.CO_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.CO_table, f'{self.bq_prefix}.fff_CO_parsed')
            push_job.result()
    
    # seg15: parsing FM
    def _parse_FM(self):
        self.FM_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Foreign_Bureau_Code', 
                     'Date_Filed', 'Name_Court', 'Court_Number', 'Industry_Code', 'Matruity_Date',
                     'Narrative_Code_1', 'Narrative_Code_2', 'Creditors_Name_Address_Amount']
        )        
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("FM ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'FM_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'FM{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.FM_table.loc[count, 'match_flag'] = fg
                    self.FM_table.loc[count, 'bus_ptnr'] = bp
                    self.FM_table.loc[count, 'file_date'] = dt
                    self.FM_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.FM_table.loc[count, 'Foreign_Bureau_Code'] = segment[3:4]
                    self.FM_table.loc[count, 'Date_Filed'] = segment[5:12]
                    self.FM_table.loc[count, 'Name_Court'] = segment[13:33]
                    self.FM_table.loc[count, 'Court_Number'] = segment[52:62]
                    self.FM_table.loc[count, 'Industry_Code'] = segment[63:65]
                    self.FM_table.loc[count, 'Matruity_Date'] = segment[66:73]
                    self.FM_table.loc[count, 'Narrative_Code_1'] = segment[73:76]
                    self.FM_table.loc[count, 'Narrative_Code_2'] = segment[77:79]
                    self.FM_table.loc[count, 'Creditors_Name_Address_Amount'] = segment[80:140]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'FM_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.FM_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.FM_table, f'{self.bq_prefix}.fff_FM_parsed')
            push_job.result()
    
    # seg16: parsing LI    
    def _parse_LI(self):
        self.LI_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Foreign_Bureau_Code', 
                     'Date_Filed', 'Name_Court', 'Court_Number', 'Amount', 'Type_Code', 'Date_Satisfied',
                     'Status_Code', 'Date_Verified', 'Narrative_Code_1', 'Narrative_Code_2',
                     'Defendant', 'Case_Number', 'Case_Number_Continued', 'Plaintiff', 'Laywer_Name_Address']
        )        
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("LI ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'LI_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'LI{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.LI_table.loc[count, 'match_flag'] = fg
                    self.LI_table.loc[count, 'bus_ptnr'] = bp
                    self.LI_table.loc[count, 'file_date'] = dt
                    self.LI_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.LI_table.loc[count, 'Foreign_Bureau_Code'] = segment[3:4]
                    self.LI_table.loc[count, 'Date_Filed'] = segment[5:12]
                    self.LI_table.loc[count, 'Name_Court'] = segment[13:33]
                    self.LI_table.loc[count, 'Court_Number'] = segment[52:62]
                    self.LI_table.loc[count, 'Amount'] = segment[63:69]
                    self.LI_table.loc[count, 'Type_Code'] = segment[70:71]
                    self.LI_table.loc[count, 'Date_Satisfied'] = segment[72:79]
                    self.LI_table.loc[count, 'Status_Code'] = segment[80:81]
                    self.LI_table.loc[count, 'Date_Verified'] = segment[82:89]
                    self.LI_table.loc[count, 'Narrative_Code_1'] = segment[90:92]
                    self.LI_table.loc[count, 'Narrative_Code_2'] = segment[93:95]
                    self.LI_table.loc[count, 'Defendant'] = segment[96:136]
                    self.LI_table.loc[count, 'Case_Number'] = segment[137:159]
                    self.LI_table.loc[count, 'Case_Number_Continued'] = segment[160:180]
                    self.LI_table.loc[count, 'Plaintiff'] = segment[181:221]
                    self.LI_table.loc[count, 'Laywer_Name_Address'] = segment[240:300]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'LI_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.LI_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.LI_table, f'{self.bq_prefix}.fff_LI_parsed')
            push_job.result()
    
    # seg17: parsing FO
    def _parse_FO(self):
        self.FO_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Foreign_Bureau_Code', 
                     'Date_Reported', 'Date_Checked', 'Narrative_Code_1', 'Narrative_Code_2',
                     'Member_Number_Member_Narrative']
        )        
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("FO ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'FO_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'FO{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.FO_table.loc[count, 'match_flag'] = fg
                    self.FO_table.loc[count, 'bus_ptnr'] = bp
                    self.FO_table.loc[count, 'file_date'] = dt
                    self.FO_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.FO_table.loc[count, 'Foreign_Bureau_Code'] = segment[3:4]
                    self.FO_table.loc[count, 'Date_Reported'] = segment[5:12]
                    self.FO_table.loc[count, 'Date_Checked'] = segment[13:20]
                    self.FO_table.loc[count, 'Narrative_Code_1'] = segment[21:23]
                    self.FO_table.loc[count, 'Narrative_Code_2'] = segment[24:26]
                    self.FO_table.loc[count, 'Member_Number_Member_Narrative'] = segment[27:67]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'FO_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.FO_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.FO_table, f'{self.bq_prefix}.fff_FO_parsed')
            push_job.result()
    
    # seg18: parsing NR
    def _parse_NR(self):
        self.NR_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Foreign_Bureau_Code', 
                     'Date_Reported', 'Person_Filling', 'Narrative_Code_1', 'Narrative_Code_2']
        )       
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("NR ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'NR_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'NR{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.NR_table.loc[count, 'match_flag'] = fg
                    self.NR_table.loc[count, 'bus_ptnr'] = bp
                    self.NR_table.loc[count, 'file_date'] = dt
                    self.NR_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.NR_table.loc[count, 'Foreign_Bureau_Code'] = segment[3:4]
                    self.NR_table.loc[count, 'Date_Reported'] = segment[5:12]
                    self.NR_table.loc[count, 'Person_Filling'] = segment[13:14]
                    self.NR_table.loc[count, 'Narrative_Code_1'] = segment[15:17]
                    self.NR_table.loc[count, 'Narrative_Code_2'] = segment[18:20]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'NR_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.NR_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.NR_table, f'{self.bq_prefix}.fff_NR_parsed')
            push_job.result()
    
    # seg19: parsing MI
    def _parse_MI(self):
        self.MI_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Foreign_Bureau_Code', 
                     'Date_Reported', 'Name_Court', 'Telephone_Area_Code', 'Telephone_Number', 
                     'Extension', 'Member_Numer', 'Action_Code', 'Date_Verified', 'Amount',
                     'Additional_Details']
        )       
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("MI ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'MI_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'MI{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.MI_table.loc[count, 'match_flag'] = fg
                    self.MI_table.loc[count, 'bus_ptnr'] = bp
                    self.MI_table.loc[count, 'file_date'] = dt
                    self.MI_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.MI_table.loc[count, 'Foreign_Bureau_Code'] = segment[3:4]
                    self.MI_table.loc[count, 'Date_Reported'] = segment[5:12]
                    self.MI_table.loc[count, 'Name_Court'] = segment[13:33]
                    self.MI_table.loc[count, 'Telephone_Area_Code'] = segment[34:37]
                    self.MI_table.loc[count, 'Telephone_Number'] = segment[38:46]
                    self.MI_table.loc[count, 'Extension'] = segment[47:51]
                    self.MI_table.loc[count, 'Member_Numer'] = segment[52:62]
                    self.MI_table.loc[count, 'Action_Code'] = segment[63:64]
                    self.MI_table.loc[count, 'Date_Verified'] = segment[65:72]
                    self.MI_table.loc[count, 'Amount'] = segment[80:122]
                    self.MI_table.loc[count, 'Additional_Details'] = segment[160:200]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'MI_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
    
        if count > 0:
            self.MI_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.MI_table, f'{self.bq_prefix}.fff_MI_parsed')
            push_job.result()
    
    # seg20: parsing TL
    def _parse_TL(self):
        self.TL_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Foreign_Bureau_Code', 
                     'Date_Filed', 'Name_Court', 'Court_Number', 'Amount', 'Industry_Code',
                     'Date_Released', 'Date_Verified', 'Narrative_Code_1', 'Narrative_Code_2',
                     'Case_Number']
        )        
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("TL ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'TL_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'TL{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.TL_table.loc[count, 'match_flag'] = fg
                    self.TL_table.loc[count, 'bus_ptnr'] = bp
                    self.TL_table.loc[count, 'file_date'] = dt
                    self.TL_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.TL_table.loc[count, 'Foreign_Bureau_Code'] = segment[3:4]
                    self.TL_table.loc[count, 'Date_Filed'] = segment[5:12]
                    self.TL_table.loc[count, 'Name_Court'] = segment[13:33]
                    self.TL_table.loc[count, 'Court_Number'] = segment[46:56]
                    self.TL_table.loc[count, 'Amount'] = segment[57:63]
                    self.TL_table.loc[count, 'Industry_Code'] = segment[64:66]
                    self.TL_table.loc[count, 'Date_Released'] = segment[67:74]
                    self.TL_table.loc[count, 'Date_Verified'] = segment[80:87]
                    self.TL_table.loc[count, 'Narrative_Code_1'] = segment[88:90]
                    self.TL_table.loc[count, 'Narrative_Code_2'] = segment[91:93]
                    self.TL_table.loc[count, 'Case_Number'] = segment[94:136]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'TL_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)

        if count > 0:
            self.TL_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.TL_table, f'{self.bq_prefix}.fff_TL_parsed')
            push_job.result()
    
    # seg21: parsing FC
    def _parse_FC(self):
        self.FC_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Foreign_Bureau_Code', 
                     'Date_Reported', 'Member_Number', 'Amount', 'Date_Filed', 'Date_Settled',
                     'Narrative_Code_1', 'Narrative_Code_2', 'Status_Code']
        )        
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("FC ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'FC_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'FC{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.FC_table.loc[count, 'match_flag'] = fg
                    self.FC_table.loc[count, 'bus_ptnr'] = bp
                    self.FC_table.loc[count, 'file_date'] = dt
                    self.FC_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.FC_table.loc[count, 'Foreign_Bureau_Code'] = segment[3:4]
                    self.FC_table.loc[count, 'Date_Reported'] = segment[5:12]
                    self.FC_table.loc[count, 'Member_Number'] = segment[13:23]
                    self.FC_table.loc[count, 'Amount'] = segment[24:30]
                    self.FC_table.loc[count, 'Date_Filed'] = segment[31:38]
                    self.FC_table.loc[count, 'Date_Settled'] = segment[39:46]
                    self.FC_table.loc[count, 'Narrative_Code_1'] = segment[47:49]
                    self.FC_table.loc[count, 'Narrative_Code_2'] = segment[50:52]
                    self.FC_table.loc[count, 'Status_Code'] = segment[53:54]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'FC_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.FC_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.FC_table, f'{self.bq_prefix}.fff_FC_parsed')
            push_job.result()
    
    # seg22: parsing GN
    def _parse_GN(self):
        self.GN_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Foreign_Bureau_Code', 
                     'Name_Court', 'Court_Number', 'Amount', 'Date_Satisfied', 'Date_Checked',
                     'Narrative_Code_1', 'Narrative_Code_2', 'Case_Number', 'Plaintiff', 
                     'Plaintiff_Continued', 'Garnishee', 'Defendant']
        )
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("GN ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'GN_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'GN{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.GN_table.loc[count, 'match_flag'] = fg
                    self.GN_table.loc[count, 'bus_ptnr'] = bp
                    self.GN_table.loc[count, 'file_date'] = dt
                    self.GN_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.GN_table.loc[count, 'Foreign_Bureau_Code'] = segment[3:4]
                    self.GN_table.loc[count, 'Name_Court'] = segment[5:12]
                    self.GN_table.loc[count, 'Court_Number'] = segment[13:33]
                    self.GN_table.loc[count, 'Amount'] = segment[57:63]
                    self.GN_table.loc[count, 'Date_Satisfied'] = segment[64:71]
                    self.GN_table.loc[count, 'Date_Checked'] = segment[72:79]
                    self.GN_table.loc[count, 'Narrative_Code_1'] = segment[80:82]
                    self.GN_table.loc[count, 'Narrative_Code_2'] = segment[83:85]
                    self.GN_table.loc[count, 'Case_Number'] = segment[86:128]
                    self.GN_table.loc[count, 'Plaintiff'] = segment[129:159]
                    self.GN_table.loc[count, 'Plaintiff_Continued'] = segment[160:172]
                    self.GN_table.loc[count, 'Garnishee'] = segment[173:213]
                    self.GN_table.loc[count, 'Defendant'] = segment[214:280]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'GN_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.GN_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.GN_table, f'{self.bq_prefix}.fff_GN_parsed')
            push_job.result()
    
    # seg23: parsing TC
    def _parse_TC(self):
        self.TC_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Foreign_Bureau_Code', 
                     'Account_Designator_Code', 'Autodata_Indicator', 'Name_Member', 
                     'Telephone_Area_Code', 'Telephone_Number', 'Extension', 'Member_Numer',
                     'Date_Reported', 'Date_Opened', 'High_Credit', 'Terms', 'Balance',
                     'Past_Due', 'Type_Code', 'Rate_Code', 'Day_Counter_30', 'Day_Counter_60',
                     'Day_Counter_90', 'Months_Reviewed', 'Date_Last_Activity', 'Account_Number',
                     'Previous_High_Rate_1', 'Previous_High_Date_1', 
                     'Previous_High_Rate_2', 'Previous_High_Date_2', 
                     'Previous_High_Rate_3', 'Previous_High_Date_3', 
                     'Narrative_Code_1', 'Narrative_Code_2']
        )       
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("TC ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'TC_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'TC{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.TC_table.loc[count, 'match_flag'] = fg
                    self.TC_table.loc[count, 'bus_ptnr'] = bp
                    self.TC_table.loc[count, 'file_date'] = dt
                    self.TC_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.TC_table.loc[count, 'Foreign_Bureau_Code'] = segment[3:4]
                    self.TC_table.loc[count, 'Account_Designator_Code'] = segment[5:6]
                    self.TC_table.loc[count, 'Autodata_Indicator'] = segment[6:7]
                    self.TC_table.loc[count, 'Name_Member'] = segment[8:28]
                    self.TC_table.loc[count, 'Telephone_Area_Code'] = segment[29:32]
                    self.TC_table.loc[count, 'Extension'] = segment[42:46]
                    self.TC_table.loc[count, 'Member_Numer'] = segment[47:57]
                    self.TC_table.loc[count, 'Date_Reported'] = segment[58:65]
                    self.TC_table.loc[count, 'Date_Opened'] = segment[66:73]
                    self.TC_table.loc[count, 'High_Credit'] = segment[74:79]
                    self.TC_table.loc[count, 'Terms'] = segment[80:84]
                    self.TC_table.loc[count, 'Balance'] = segment[85:90]
                    self.TC_table.loc[count, 'Past_Due'] = segment[91:96]
                    self.TC_table.loc[count, 'Type_Code'] = segment[97:98]
                    self.TC_table.loc[count, 'Rate_Code'] = segment[98:99]
                    self.TC_table.loc[count, 'Day_Counter_30'] = segment[100:102]
                    self.TC_table.loc[count, 'Day_Counter_60'] = segment[103:105]
                    self.TC_table.loc[count, 'Day_Counter_90'] = segment[106:108]
                    self.TC_table.loc[count, 'Months_Reviewed'] = segment[109:111]
                    self.TC_table.loc[count, 'Date_Last_Activity'] = segment[112:119]
                    self.TC_table.loc[count, 'Account_Number'] = segment[120:135]
                    self.TC_table.loc[count, 'Previous_High_Rate_1'] = segment[161:162]
                    self.TC_table.loc[count, 'Previous_High_Date_1'] = segment[163:170]
                    self.TC_table.loc[count, 'Previous_High_Rate_2'] = segment[172:173]
                    self.TC_table.loc[count, 'Previous_High_Date_2'] = segment[174:181]
                    self.TC_table.loc[count, 'Previous_High_Rate_3'] = segment[183:184]
                    self.TC_table.loc[count, 'Previous_High_Date_3'] = segment[185:192]
                    self.TC_table.loc[count, 'Narrative_Code_1'] = segment[196:198]
                    self.TC_table.loc[count, 'Narrative_Code_2'] = segment[199:201]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'TC_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.TC_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.TC_table, f'{self.bq_prefix}.fff_TC_parsed')
            push_job.result()
    
    # seg24: parsing NT
    def _parse_NT(self):
        self.NT_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Date_Reported',
                     'Type_Code', 'Rating_Code_0_Greater', 'Rating_Code_Less_Than_0', 
                     'Date_Opened', 'Narrative_Code_1', 'Narrative_Code_2', 
                     'Customer_Narrative', 'High_Credit_Amount', 'Balance', 'Past_Due_Amount']
        )       
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("NT ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'NT_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'NT{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.NT_table.loc[count, 'match_flag'] = fg
                    self.NT_table.loc[count, 'bus_ptnr'] = bp
                    self.NT_table.loc[count, 'file_date'] = dt
                    self.NT_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.NT_table.loc[count, 'Date_Reported'] = segment[3:10]
                    self.NT_table.loc[count, 'Type_Code'] = segment[11:12]
                    self.NT_table.loc[count, 'Rating_Code_0_Greater'] = segment[13:14]
                    self.NT_table.loc[count, 'Rating_Code_Less_Than_0'] = segment[15:16]
                    self.NT_table.loc[count, 'Date_Opened'] = segment[17:24]
                    self.NT_table.loc[count, 'Narrative_Code_1'] = segment[25:27]
                    self.NT_table.loc[count, 'Narrative_Code_2'] = segment[28:30]
                    self.NT_table.loc[count, 'Customer_Narrative'] = segment[31:71]
                    self.NT_table.loc[count, 'High_Credit_Amount'] = segment[71:78]
                    self.NT_table.loc[count, 'Balance'] = segment[80:86]
                    self.NT_table.loc[count, 'Past_Due_Amount'] = segment[87:93]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'NT_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.NT_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.NT_table, f'{self.bq_prefix}.fff_NT_parsed')
            push_job.result()
    
    # seg25: parsing CS
    def _parse_CS(self):
        self.CS_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Foreign_Bureau_Code', 
                     'Date_Reported', 'Name_Member', 'Telephone_Area_Code', 'Telephone_Number', 
                     'Extension', 'Member_Numer', 'Date_Opened', 'Amount', 'Type_Account', 
                     'Narrative_Code_1', 'Status_Code', 'NSF_Information', 'Account_Number']
        )        
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("CS ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'CS_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'CS{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.CS_table.loc[count, 'match_flag'] = fg
                    self.CS_table.loc[count, 'bus_ptnr'] = bp
                    self.CS_table.loc[count, 'file_date'] = dt
                    self.CS_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.CS_table.loc[count, 'Foreign_Bureau_Code'] = segment[5:12]
                    self.CS_table.loc[count, 'Name_Member'] = segment[13:33]
                    self.CS_table.loc[count, 'Telephone_Area_Code'] = segment[34:37]
                    self.CS_table.loc[count, 'Telephone_Number'] = segment[38:46]
                    self.CS_table.loc[count, 'Extension'] = segment[47:51]
                    self.CS_table.loc[count, 'Member_Numer'] = segment[52:62]
                    self.CS_table.loc[count, 'Date_Opened'] = segment[63:70]
                    self.CS_table.loc[count, 'Amount'] = segment[80:95]
                    self.CS_table.loc[count, 'Type_Account'] = segment[96:97]
                    self.CS_table.loc[count, 'Narrative_Code_1'] = segment[98:100]
                    self.CS_table.loc[count, 'Status_Code'] = segment[101:102]
                    self.CS_table.loc[count, 'NSF_Information'] = segment[103:118]
                    self.CS_table.loc[count, 'Account_Number'] = segment[119:134]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'CS_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.CS_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.CS_table, f'{self.bq_prefix}.fff_CS_parsed')
            push_job.result()
     
    # seg26: parsing FB
    def _parse_FB(self):
        self.FB_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Date_Reported', 
                     'Foreign_Bureau_Code', 'City_Narrative', 'Province_Narrative']
        )        
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("FB ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'FB_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'FB{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.FB_table.loc[count, 'match_flag'] = fg
                    self.FB_table.loc[count, 'bus_ptnr'] = bp
                    self.FB_table.loc[count, 'file_date'] = dt
                    self.FB_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.FB_table.loc[count, 'Date_Reported'] = segment[3:10]
                    self.FB_table.loc[count, 'Foreign_Bureau_Code'] = segment[11:12]
                    self.FB_table.loc[count, 'City_Narrative'] = segment[13:31]
                    self.FB_table.loc[count, 'Province_Narrative'] = segment[32:72]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'FB_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.FB_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.FB_table, f'{self.bq_prefix}.fff_FB_parsed')
            push_job.result()
     
    # seg27: parsing FI
    def _parse_FI(self):
        self.FI_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Date_Inquiry', 
                     'City_Narrative', 'Province_Narrative']
        )       
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("Fi ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'FI_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'FI{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.FI_table.loc[count, 'match_flag'] = fg
                    self.FI_table.loc[count, 'bus_ptnr'] = bp
                    self.FI_table.loc[count, 'file_date'] = dt
                    self.FI_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.FI_table.loc[count, 'Date_Inquiry'] = segment[3:13]
                    self.FI_table.loc[count, 'City_Narrative'] = segment[14:32]
                    self.FI_table.loc[count, 'Province_Narrative'] = segment[33:53]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'FI_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.FI_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.FI_table, f'{self.bq_prefix}.fff_FI_parsed')
            push_job.result()
    
    # seg28: parsing LO
    def _parse_LO(self):
        self.LO_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Date_Reported', 
                     'Name_Member', 'Telephone_Area_Code', 'Telephone_Number', 
                     'Extension', 'Member_Numer', 'Type_Code']
        )        
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("LO ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'LO_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'LO{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.LO_table.loc[count, 'match_flag'] = fg
                    self.LO_table.loc[count, 'bus_ptnr'] = bp
                    self.LO_table.loc[count, 'file_date'] = dt
                    self.LO_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.LO_table.loc[count, 'Date_Reported'] = segment[3:10]
                    self.LO_table.loc[count, 'Name_Member'] = segment[11:31]
                    self.LO_table.loc[count, 'Telephone_Area_Code'] = segment[32:35]
                    self.LO_table.loc[count, 'Telephone_Number'] = segment[36:44]
                    self.LO_table.loc[count, 'Extension'] = segment[45:49]
                    self.LO_table.loc[count, 'Member_Numer'] = segment[50:60]
                    self.LO_table.loc[count, 'Type_Code'] = segment[61:62]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'LO_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.LO_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.LO_table, f'{self.bq_prefix}.fff_LO_parsed')
            push_job.result()
    
    # seg29: parsing IQ
    def _parse_IQ(self):
        self.IQ_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Date_Inquiry', 
                     'Name_Member', 'Telephone_Area_Code', 'Telephone_Number', 
                     'Extension', 'Member_Numer']
        )       
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("IQ ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'IQ_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'IQ{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.IQ_table.loc[count, 'match_flag'] = fg
                    self.IQ_table.loc[count, 'bus_ptnr'] = bp
                    self.IQ_table.loc[count, 'file_date'] = dt
                    self.IQ_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.IQ_table.loc[count, 'Date_Inquiry'] = segment[3:13]
                    self.IQ_table.loc[count, 'Name_Member'] = segment[14:34]
                    self.IQ_table.loc[count, 'Telephone_Area_Code'] = segment[35:38]
                    self.IQ_table.loc[count, 'Telephone_Number'] = segment[39:47]
                    self.IQ_table.loc[count, 'Extension'] = segment[48:52]
                    self.IQ_table.loc[count, 'Member_Numer'] = segment[53:63]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'IQ_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.IQ_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.IQ_table, f'{self.bq_prefix}.fff_IQ_parsed')
            push_job.result()
    
    # seg30: parsing CD
    def _parse_CD(self):
        self.CD_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Date_Reported', 
                     'Date_Purged', 'Declaration', 'Declaration_Continued_1', 'Declaration_Continued_2',
                     'Declaration_Continued_3', 'Declaration_Continued_4', 'Declaration_Continued_End']
        )       
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("CD ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'CD_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'CD{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.CD_table.loc[count, 'match_flag'] = fg
                    self.CD_table.loc[count, 'bus_ptnr'] = bp
                    self.CD_table.loc[count, 'file_date'] = dt
                    self.CD_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.CD_table.loc[count, 'Date_Reported'] = segment[3:10]
                    self.CD_table.loc[count, 'Date_Purged'] = segment[11:18]
                    self.CD_table.loc[count, 'Declaration'] = segment[19:79]
                    self.CD_table.loc[count, 'Declaration_Continued_1'] = segment[80:158]
                    self.CD_table.loc[count, 'Declaration_Continued_2'] = segment[160:238]
                    self.CD_table.loc[count, 'Declaration_Continued_3'] = segment[240:318]
                    self.CD_table.loc[count, 'Declaration_Continued_4'] = segment[320:398]
                    self.CD_table.loc[count, 'Declaration_Continued_End'] = segment[400:428]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'CD_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
    
        if count > 0:
            self.CD_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.CD_table, f'{self.bq_prefix}.fff_CD_parsed')
            push_job.result()
    
    # seg31: parsing BS
    def _parse_BS(self):
        self.BS_table = pd.DataFrame(
            columns=['match_flag', 'bus_ptnr', 'file_date', 'Record_Code', 'Product_Score', 
                     'First_Reason_Code', 'Second_Reason_Code', 'Third_Reason_Code', 'Fourth_Reason_Code',
                     'Reject_Message_Code', 'Reserved', 'Product_Identifier']
        )        
        self.data['segment_indices'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("BS ", x)])
        count = 0
        for ncol in self.data.index:
            idx_list = self.data.loc[ncol, 'segment_indices']
            n_record = len(idx_list)
            bp = self.data.loc[ncol, 'id']
            dt = self.data.loc[ncol, 'file_date']
            self.data.loc[ncol, 'BS_num_records'] = n_record
            if n_record != 0:
                id = self.data.loc[ncol, 'id']
                mfile = self.data.loc[ncol, 'mfile']
                flag = ''
                for i in idx_list:
                    segment = mfile[i:]
                    fg = f'BS{id}{self.year}{str(self.month).zfill(2)}{str(count).zfill(10)}'
                    self.BS_table.loc[count, 'match_flag'] = fg
                    self.BS_table.loc[count, 'bus_ptnr'] = bp
                    self.BS_table.loc[count, 'file_date'] = dt
                    self.BS_table.loc[count, 'Record_Code'] = segment[0:2]
                    self.BS_table.loc[count, 'Product_Score'] = segment[3:8]
                    self.BS_table.loc[count, 'First_Reason_Code'] = segment[9:11]
                    self.BS_table.loc[count, 'Second_Reason_Code'] = segment[12:14]
                    self.BS_table.loc[count, 'Third_Reason_Code'] = segment[15:17]
                    self.BS_table.loc[count, 'Fourth_Reason_Code'] = segment[18:20]
                    self.BS_table.loc[count, 'Reject_Message_Code'] = segment[21:22]
                    self.BS_table.loc[count, 'Reserved'] = segment[26:28]
                    self.BS_table.loc[count, 'Product_Identifier'] = segment[77:79]
                    count += 1
                    flag += f'{fg}, '
                self.data.loc[ncol, 'BS_ref_flag'] = flag[:-2]
        self.data.drop(columns='segment_indices', inplace=True)
        
        if count > 0:
            self.BS_table.reset_index(drop=True)
            push_job = self.client.load_table_from_dataframe(self.BS_table, f'{self.bq_prefix}.fff_BS_parsed')
            push_job.result()

if __name__ == '__main__':
    # This is designed as in a monthly running frequency.
    # Each time running this code, designate the year and the month of the data want to be retrieved
    # Steps:
    #     1. object instantiation with year and month
    #     2. call fetch_date_from_google_bigquery()
    #     3. call push_table_to_google_bigquery()
    # Attention: 
    #     Although according to the official document, the default setting for the BigQuery's write_diposition is WRITE_EMPTY,
    #     based on my experiment, the setting seems to be WRITE_APPEND.
    #     In order not to mess up everything, I didn't reset the write_diposition value.

    fff_parser = FFFParser(2024, 1)
    fff_parser.fetch_date_from_google_bigquery()
    fff_parser.push_table_to_google_bigquery()