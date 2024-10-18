import re
import time
import warnings
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from datetime import date
from typing import Optional, Tuple 
# from google.auth.exceptions import RefreshError 

# global setting
job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
)
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)
warnings.filterwarnings('ignore', category=UserWarning, message='.*quota project.*')


# A class that contains all common filters which used to guarantee the correctness of parsing
class FilterAndConverter:
    @classmethod
    def convert_amount_from_str(cls, s: str):
        s = s.strip()
        if s == '':
            return pd.NA
        if s[0] == '$':
            s = s[1:]

        try:
            if s[-1] == 'K':
                return int(s[:-1]) * 1000
            elif s[-1] == 'M':
                return int(s[:-1]) * 1000000
            else:
                return int(s)
        except:
            return pd.NA
        
    @classmethod
    def convert_int_with_missing(cls, s: str):
        s = s.strip()
        if s == '':
            return pd.NA
        else:
            try:
                return int(s)
            except:
                return pd.NA
        
    @classmethod
    def convert_float_with_missing(cls, s: str):
        s = s.strip()
        if s == '':
            return pd.NA
        else:
            try:
                return float(s)
            except:
                return pd.NA
        
    @classmethod
    def calc_length_with_strip(cls, s: str) -> int:
        return len(s.strip())
        
    @classmethod
    def filter_member_number(cls, s: str) -> bool:
        # '999XX99999' and allow none    
        s = s.strip()
        check = s[:3].isdigit() and s[3:5].isalpha() and s[5:].isdigit()
        check = check or s == ''
        return check
    
    @classmethod
    def filter_6digits_date(cls, s: str) -> bool:
        # 'YYYYMM' and allow none 
        s = s.strip()
        check = (s.isdigit() and len(s) == 6) or s == ''
        return check
    
    @classmethod
    def filter_valid_name(cls, s: str) -> bool:
        if s.strip() == '':
            return True
        else:
            if s[0] == ' ':
                return False
            elif ('/' in s) or ('*' in s) or bool(re.search(r'\d', s)):
                return False
            else:
                return True
            
    @classmethod
    def filter_first_not_null(cls, s: str) -> bool:
        if s.strip() != '':
            return s[0] != ' '
        else:
            return True
        
    @classmethod
    def filter_industry_code(cls, s: str) -> bool:
        if s.strip() != '':
            return len(s.strip()) == 2 and  s.strip().isalpha()
        else:
            return True
        
    @classmethod
    def convert_8digits_date(cls, s:str):
        s = s.strip()
        if s == '':
            return None
        else:
            try:
                dt = date(int(s[6:]), int(s[:2]), int(s[3:5]))
            except:
                return None
        return dt


# The parsing class
class FFFParser:
    def __init__(self, begin_year: int, begin_month: int, 
                 end_year: Optional[int] = None, end_month: Optional[int] = None,
                 which_tables: list = None, push_header: bool = True, debug_mode: bool=False,
                 project_id: str, dataset_id: str):
        self.begin_year = begin_year
        self.begin_month = begin_month
        self.end_year = end_year
        self.end_month = end_month
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.push_header = push_header
        self.debug_mode = debug_mode
        
        self.fff_name = 'xxx'
        self.bq_prefix = f'{self.project_id}.{self.dataset_id}'
        self.client = bigquery.Client(project=project_id)
        
        if which_tables is None:
            self.seg_names = ['address', 'name', 'death', 'employment', 'other_income', 'bankruptcy', 'collection', 
                              'secured_loan', 'legal_item', 'marital_item', 'garnishment', 'trade_check', 
                              'chequing_saving', 'foreign_bureau', 'inquries', 'locate_special_service', 
                              'consumer_declaration', 'bureau_score']  
        else:
            self.seg_names = which_tables
            
        self.error_log_info = {
            'year': [self.begin_year, self.end_year],
            'month': [self.begin_month, self.end_month],
            'need_pushed': self.seg_names + ['header']  if push_header else self.seg_names,
            'already_pushed': [], 
            'left_pushed': self.seg_names + ['header'] if push_header else self.seg_names
        }

        self.header_cols_dict = {
            'report_type': (0, 4),
            'customer_reference_no': (5, 17), 
            'member_no': (18, 28),
            'consumer_referral_no': (29, 32),
            'ecoa_inquiry_type': (34, 35),
            'output_format_code': (36, 37),
            'hit_no_hit_designator': (41, 42), 
            'file_since_date': (43, 53), 
            'last_activity_date': (54, 64),
            'this_report_date': (65, 75),
            'last_name': (80, 105),
            "first_name": (106, 121),
            'middle_name_or_initial': (122, 137),
            'suffixs': (138, 140),
            'spouses_name': (141, 156),
            'record_code_ss': (160, 162),
            'subjects_sin': (162, 171),
            'subjects_birth_age_date': (172, 182), 
            'record_code_so': (190, 192), 
            'total_no_of_inquiries': (202, 205),
            'warning_message': (208, 209),
            'alert_indicator_flag': (210, 211), 
            'segment_counter': (240, 302), 
            'alert_flag': (312, 314), 
            'deposit_flag': (315, 316), 
            'safescan_byte_1': (317, 318),
            'safescan_is_byte_2': (318, 319)
        }
        self.column_taboo = ['check', 'file_raw_content']
        self._fetch_data_from_google_bigquery()

    def _fetch_data_from_google_bigquery(self):
        fetch_query = self._construct_fetch_query()
        query_job = self.client.query(fetch_query)    
        fetch_job = query_job.result()
        self.raw_data = fetch_job.to_dataframe()
        
        self.data = self.raw_data[['id', 'file_name', 'file_date', 'business_partner_id', 'file_raw_content']].copy()
        self.data = self.data[~self.data['file_raw_content'].isna()]
        self.data['check'] = self.data.file_raw_content.apply(lambda x: 'FULL' in x)
        self.data = self.data.loc[self.data.check]
        self.data['mfile'] = None
        for col in self.header_cols_dict.keys():
            self.data[col] = None
            
        if self.end_year is not None and self.end_month is not None:
            print(f'******************** FFF data ({self.begin_year}.{self.begin_month} to {self.end_year}.{self.end_month}) has been retrieved ! ********************')
        else:
            print(f'******************** FFF data ({self.begin_year}.{self.begin_month}) has been retrieved ! ********************')
            
    def push_tables_to_google_bigquery(self, parse_header: bool = True):
        if parse_header:
            self._parse_header()
        
        # push segment tables         
        for seg in self.seg_names:
            # exec(f'self._parse_{seg}()', {'self': self})
            self.error_log_info['already_pushed'].append(seg)
            self.error_log_info['left_pushed'].remove(seg)
            time.sleep(0.1)
            
        # push the header table
        if self.push_header:
            self.data.drop(columns=self.column_taboo, inplace=True)
            self.data.reset_index(drop=True)
            header_scheme = [
                SchemaField('id', 'STRING'),
                SchemaField('file_name', 'STRING'),
                SchemaField('file_date', 'DATE'),
                SchemaField('business_partner_id', 'STRING'),
                SchemaField('mfile', 'STRING'),
                SchemaField('report_type', 'STRING'),
                SchemaField('customer_reference_no', 'STRING'),
                SchemaField('member_no', 'STRING'),
                SchemaField('consumer_referral_no', 'STRING'),
                SchemaField('ecoa_inquiry_type', 'STRING'),
                SchemaField('output_format_code', 'STRING'),
                SchemaField('hit_no_hit_designator', 'STRING'),
                SchemaField('file_since_date', 'DATE'),
                SchemaField('last_activity_date', 'DATE'),
                SchemaField('this_report_date', 'DATE'),
                SchemaField('last_name', 'STRING'),
                SchemaField('first_name', 'STRING'),
                SchemaField('middle_name_or_initial', 'STRING'),
                SchemaField('suffixs', 'STRING'),
                SchemaField('record_code_ss', 'STRING'),
                SchemaField('subjects_sin', 'STRING'),
                SchemaField('subjects_birth_age_date', 'DATE'),
                SchemaField('record_code_so', 'STRING'),
                SchemaField('total_no_of_inquiries', 'STRING'),
                SchemaField('warning_message', 'STRING'),
                SchemaField('alert_indicator_flag', 'STRING'),
                SchemaField('segment_counter', 'STRING'),
                SchemaField('alert_flag', 'STRING'),
                SchemaField('deposit_flag', 'STRING'),
                SchemaField('safescan_byte_1', 'STRING'),
                SchemaField('subjects_sin', 'STRING'),
                SchemaField('safescan_is_byte_2', 'STRING')
            ]
            self.data['file_since_date'] = self.data['file_since_date'].apply(FilterAndConverter.convert_8digits_date)
            self.data['last_activity_date'] = self.data['last_activity_date'].apply(FilterAndConverter.convert_8digits_date)
            self.data['this_report_date'] = self.data['this_report_date'].apply(FilterAndConverter.convert_8digits_date)
            self.data['subjects_birth_age_date'] = self.data['subjects_birth_age_date'].apply(FilterAndConverter.convert_8digits_date)
            if not self.debug_mode: 
                push_job = self.client.load_table_from_dataframe(self.data, f'{self.bq_prefix}.fff_segment_0_header',
                                                                 job_config=bigquery.LoadJobConfig(schema=header_scheme))  
                push_job.result()
            else:
                time.sleep(1)
            self.error_log_info['already_pushed'].append('header')
            self.error_log_info['left_pushed'].remove('header')
            print(f'header table has been pushed to BigQuery @ {self.bq_prefix}')
         
        if self.end_year is not None and self.end_month is not None:
            print(f'******************** Push ({self.begin_year}.{self.begin_month} to {self.end_year}.{self.end_month}) complete ! ********************')
        else:
            print(f'******************** Push ({self.begin_year}.{self.begin_month}) complete ! ********************')
            
    def restart_from_break(self):
        if len(self.error_log_info['left_pushed']) == 0:
            print('Already complete!')
            return 
        
        self.seg_names = self.error_log_info['left_pushed'].copy()
        if 'header' in self.error_log_info['left_pushed']:
            self.seg_names.remove('header')
            self.push_header = True
        else:
            self.push_header = False
            
        if 'mfile' in list(self.data.columns):
            self.push_tables_to_google_bigquery(parse_header=False)
        else:
            self.push_tables_to_google_bigquery()      
                
    def _push_seg_table(self, table: pd.DataFrame, table_len: int, seg_name: str, schema: list):
        if table_len > 0:
            table.reset_index(drop=True)
            if not self.debug_mode:
                push_job = self.client.load_table_from_dataframe(table, f'{self.bq_prefix}.fff_segment_{seg_name}', 
                                                                 job_config=bigquery.LoadJobConfig(schema=schema))
                push_job.result()
            else:
                time.sleep(1)
            print(f'{seg_name} table has been pushed to BigQuery @ {self.bq_prefix}')
        
    def _construct_fetch_query(self) -> str:
        if self.end_year is not None and self.end_year is not None:
            fetch_query = f"""
                SELECT * FROM `{self.fff_name}`
                WHERE file_date >= DATE(SAFE_CAST({self.begin_year} AS INT64), SAFE_CAST({self.begin_month} AS INT64), 1) AND
                file_date <= LAST_DAY(DATE(SAFE_CAST({self.end_year} AS INT64), SAFE_CAST({self.end_month} AS INT64), 1))
                ORDER BY business_partner_id, file_date
            """
        else:
            fetch_query = f"""
                SELECT * FROM `{self.fff_name}`
                WHERE file_date >= DATE(SAFE_CAST({self.begin_year} AS INT64), SAFE_CAST({self.begin_month} AS INT64), 1) AND
                file_date <= LAST_DAY(DATE(SAFE_CAST({self.begin_year} AS INT64), SAFE_CAST({self.begin_month} AS INT64), 1))
                ORDER BY business_partner_id, file_date
                LIMIT 1000
            """
            
        if self.debug_mode:
            fetch_query += ' LIMIT 200'
            
        return fetch_query
    
    def _parse_entry_details(self, ncol: int) -> Tuple:
        bp = self.data.loc[ncol, 'id']
        dt = self.data.loc[ncol, 'file_date']
        dt_str = str(dt)[:4] + str(dt)[5:7] + str(dt)[8:]
        mfile = self.data.loc[ncol, 'mfile']
        return bp, dt, dt_str, mfile
    
    def _parse_seg_index(self, ncol: int, seg_list: list) -> list:
        output = []
        for i, seg in enumerate(seg_list):
            idx = self.data.loc[ncol, seg]
            output.append(idx)
            output.append(len(idx))
        return output
         
    # parsing header    
    def _parse_header(self):
        self.data['mfile'] = self.data['file_raw_content'].apply(lambda x: x[x.index('FULL'):])
        for key, val in self.header_cols_dict.items():
            self.data[key] = self.data.mfile.apply(lambda x: x[val[0]:val[1]])
        # self.column_taboo.append('mfile')
       
    # 1. parsing address    
    def _parse_address(self):
        self.addr = pd.DataFrame(
            columns=['bus_ptnr', 'file_date', 'street_number', 'street_name_direction_apartment', 'city', 'province', 
                     'postal_code', 'residence_since', 'indicator_code', 'segment_code', 'segment_description', 'order_in_segment']
        )
        addr_scheme = [
            SchemaField('bus_ptnr', 'STRING'),
            SchemaField('file_date', 'DATE'),
            SchemaField('street_number', 'STRING'),
            SchemaField('street_name_direction_apartment', 'STRING'),
            SchemaField('city', 'STRING'),
            SchemaField('province', 'STRING'),
            SchemaField('postal_code', 'STRING'),
            SchemaField('residence_since', 'STRING'),
            SchemaField('indicator_code', 'STRING'),
            SchemaField('segment_code', 'STRING'),
            SchemaField('segment_description', 'STRING'),
            SchemaField('order_in_segment', 'INT64')
        ]
        self.data['idx_CA'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(' CA ', x)])    
        self.data['idx_FA'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(' FA ', x)])
        self.data['idx_F2'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(' F2 ', x)])
        count = 0
        for ncol in self.data.index:
            bp, dt, dt_str, mfile = self._parse_entry_details(ncol=ncol)
            ca_index, ca_records, fa_index, fa_records, f2_index, f2_records = self._parse_seg_index(ncol=ncol, seg_list = ['idx_CA', 'idx_FA', 'idx_F2'])
            if ca_records + fa_records + f2_records > 0:
                if ca_records != 0:
                    count_ca = 1
                    for i in ca_index:
                        segment = mfile[i:]
                        self.addr.loc[count, 'bus_ptnr'] = bp
                        self.addr.loc[count, 'file_date'] = dt
                        self.addr.loc[count, 'street_number'] = segment[3:13]
                        self.addr.loc[count, 'street_name_direction_apartment'] = segment[14:40]
                        self.addr.loc[count, 'city'] = segment[80:100]
                        self.addr.loc[count, 'province'] = segment[101:103]
                        self.addr.loc[count, 'postal_code'] = segment[104:110]
                        self.addr.loc[count, 'residence_since'] = segment[114:118] + segment[111:113]
                        self.addr.loc[count, 'indicator_code'] = segment[118:119]
                        self.addr.loc[count, 'segment_code'] = 'CA'
                        self.addr.loc[count, 'segment_description'] = 'current address'
                        self.addr.loc[count, 'order_in_segment'] = count_ca
                        count += 1
                        count_ca += 1
                if fa_records != 0:
                    count_fa = 1
                    for i in fa_index:
                        segment = mfile[i:]
                        self.addr.loc[count, 'bus_ptnr'] = bp
                        self.addr.loc[count, 'file_date'] = dt
                        self.addr.loc[count, 'street_number'] = segment[3:13]
                        self.addr.loc[count, 'street_name_direction_apartment'] = segment[14:40]
                        self.addr.loc[count, 'city'] = segment[80:100]
                        self.addr.loc[count, 'province'] = segment[101:103]
                        self.addr.loc[count, 'postal_code'] = segment[104:110]
                        self.addr.loc[count, 'residence_since'] = segment[114:118] + segment[111:113]
                        self.addr.loc[count, 'indicator_code'] = segment[118:119]
                        self.addr.loc[count, 'segment_code'] = 'FA'
                        self.addr.loc[count, 'segment_description'] = 'former address'
                        self.addr.loc[count, 'order_in_segment'] = count_fa
                        count += 1
                        count_fa += 1
                if f2_records != 0:
                    count_f2 = 1
                    for i in f2_index:
                        segment = mfile[i:]
                        self.addr.loc[count, 'bus_ptnr'] = bp
                        self.addr.loc[count, 'file_date'] = dt
                        self.addr.loc[count, 'street_number'] = segment[3:13]
                        self.addr.loc[count, 'street_name_direction_apartment'] = segment[14:40]
                        self.addr.loc[count, 'city'] = segment[80:100]
                        self.addr.loc[count, 'province'] = segment[101:103]
                        self.addr.loc[count, 'postal_code'] = segment[104:110]
                        self.addr.loc[count, 'residence_since'] = segment[114:118] + segment[111:113]
                        self.addr.loc[count, 'indicator_code'] = segment[118:119]
                        self.addr.loc[count, 'segment_code'] = 'F2'
                        self.addr.loc[count, 'segment_description'] = 'former address'
                        self.addr.loc[count, 'order_in_segment'] = count_f2
                        count += 1
                        count_f2 += 1
        self.column_taboo += ['idx_CA', 'idx_FA', 'idx_F2']
        self.addr['check1'] = self.addr.city.apply(FilterAndConverter.calc_length_with_strip)
        self.addr['check2'] = self.addr.province.apply(FilterAndConverter.calc_length_with_strip)
        self.addr['check3'] = self.addr.postal_code.apply(FilterAndConverter.calc_length_with_strip)
        self.addr['check4'] = self.addr.residence_since.apply(FilterAndConverter.filter_6digits_date)
        self.addr = self.addr.loc[((self.addr.check1 <= 4) & (self.addr.check2 == 2) & (self.addr.check3 == 6) & self.addr.check4)]
        self.addr.drop(columns = ['check1', 'check2', 'check3', 'check4'], inplace=True)
        self._push_seg_table(table=self.addr, table_len=len(self.addr), seg_name='1_2_3_address', schema=addr_scheme)        # <--- here   
    
    # 2. parsing names
    def _parse_name(self):
        self.names = pd.DataFrame(
            columns=['bus_ptnr', 'file_date', 'last_name', 'first_name', 'middle_name_initial', 'suffix', 
                     'spouse_name', 'legal_name_change', 'segment_code', 'segment_description', 'order_in_segment']
        )
        name_scheme = [
            SchemaField('bus_ptnr', 'STRING'),
            SchemaField('file_date', 'DATE'),
            SchemaField('last_name', 'STRING'),
            SchemaField('first_name', 'STRING'),
            SchemaField('middle_name_initial', 'STRING'),
            SchemaField('suffix', 'STRING'),
            SchemaField('spouse_name', 'STRING'),
            SchemaField('legal_name_change', 'STRING'),
            SchemaField('segment_code', 'STRING'),
            SchemaField('segment_description', 'STRING'),
            SchemaField('order_in_segment', 'INT64')
        ]
        self.data['idx_AK'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" AK ", x)])
        self.data['idx_FN'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("FN ", x)])
        count = 0
        for ncol in self.data.index:
            bp, dt, dt_str, mfile = self._parse_entry_details(ncol=ncol)
            ak_index, ak_records, fn_index, fn_records= self._parse_seg_index(ncol=ncol, seg_list = ['idx_AK', 'idx_FN'])
            if ak_records + fn_records > 0:
                if ak_records != 0:
                    count_ak = 1
                    for i in ak_index:
                        segment = mfile[i:]
                        self.names.loc[count, 'bus_ptnr'] = bp
                        self.names.loc[count, 'file_date'] = dt
                        self.names.loc[count, 'last_name'] = segment[3:28]
                        self.names.loc[count, 'first_name'] = segment[29:44]
                        self.names.loc[count, 'middle_name_initial'] = segment[45:60]
                        self.names.loc[count, 'suffix'] = segment[61:63]
                        self.names.loc[count, 'spouse_name'] = segment[80:95]
                        self.names.loc[count, 'legal_name_change'] = segment[96:97]
                        self.names.loc[count, 'segment_code'] = 'AK'
                        self.names.loc[count, 'segment_description'] = 'also known as'
                        self.names.loc[count, 'order_in_segment'] = count_ak
                        count += 1
                        count_ak += 1
                if fn_records != 0:
                    count_fn = 1
                    for i in fn_index:
                        segment = mfile[i:]
                        self.names.loc[count, 'bus_ptnr'] = bp
                        self.names.loc[count, 'file_date'] = dt
                        self.names.loc[count, 'last_name'] = segment[3:28]
                        self.names.loc[count, 'first_name'] = segment[29:44]
                        self.names.loc[count, 'middle_name_initial'] = segment[45:60]
                        self.names.loc[count, 'suffix'] = segment[61:63]
                        self.names.loc[count, 'spouse_name'] = segment[80:95]
                        self.names.loc[count, 'legal_name_change'] = segment[96:97]
                        self.names.loc[count, 'segment_code'] = 'FN'
                        self.names.loc[count, 'segment_description'] = 'also known as'
                        self.names.loc[count, 'order_in_segment'] = count_fn
                        count += 1
                        count_fn += 1
        self.column_taboo += ['idx_AK', 'idx_FN']
        self.names['check1'] = self.names.last_name.apply(FilterAndConverter.filter_valid_name)
        self.names['check2'] = self.names.first_name.apply(FilterAndConverter.filter_valid_name)
        self.names['check3'] = self.names.middle_name_initial.apply(FilterAndConverter.filter_valid_name)
        self.names['check4'] = self.names.spouse_name.apply(FilterAndConverter.filter_valid_name)
        self.names['check5'] = self.names.suffix.apply(lambda x: x in ['SR', 'JR', '1 ', '2 ', '3 ', '4 ', 'XX', '  '] or x == '')
        self.names['check6'] = self.names.legal_name_change.apply(lambda x: x == 'L' or x == ' ')
        self.names = self.names.loc[self.names.check1 & self.names.check2 & self.names.check3 & self.names.check4 & self.names.check5 & self.names.check6]
        self.names.drop(columns = ['check1', 'check2', 'check3', 'check4', 'check5', 'check6'], inplace=True)
        self._push_seg_table(table=self.names, table_len=count, seg_name='4_5_name', schema=name_scheme)      
             
    # 3. parsing death
    def _parse_death(self):
        self.death = pd.DataFrame(
            columns=['bus_ptnr', 'file_date', 'subject_death_date', 'segment_code', 'segment_description', 'order_in_segment']
        )
        death_scheme = [
            SchemaField('bus_ptnr', 'STRING'),
            SchemaField('file_date', 'DATE'),
            SchemaField('subject_death_date', 'STRING'),
            SchemaField('segment_code', 'STRING'),
            SchemaField('segment_description', 'STRING'),
            SchemaField('order_in_segment', 'INT64')
        ]
        self.data['idx_DT'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("DT ", x)])
        count = 0
        for ncol in self.data.index:
            bp, dt, dt_str, mfile = self._parse_entry_details(ncol=ncol)
            dt_index, dt_records = self._parse_seg_index(ncol=ncol, seg_list = ['idx_DT'])
            if dt_records != 0:
                count_dt = 1
                for i in dt_index:
                    segment = mfile[i:]
                    self.death.loc[count, 'bus_ptnr'] = bp
                    self.death.loc[count, 'file_date'] = dt
                    self.death.loc[count, 'subject_death_date'] = segment[6:10] + segment[3:5]
                    self.death.loc[count, 'segment_code'] = 'DT'
                    self.death.loc[count, 'segment_description'] = 'death'
                    self.death.loc[count, 'order_in_segment'] = count_dt
                    count += 1
                    count_dt += 1
        self.column_taboo.append('idx_DT')
        self.death['check1'] = self.death.subject_death_date.apply(FilterAndConverter.filter_6digits_date)
        self.death['check2'] = self.death.subject_death_date.apply(lambda x: len(x.strip()) > 0 ) 
        self.death = self.death.loc[self.death.check1 & self.death.check2]
        self.death.drop(columns = ['check1', 'check2'], inplace=True)
        self._push_seg_table(table=self.death, table_len=len(self.death), seg_name='6_death', schema=death_scheme)      
    
    # 4. parsing employment
    def _parse_employment(self):
        self.empl = pd.DataFrame(
            columns=['bus_ptnr', 'file_date', 'occupation', 'employer', 'city_of_employment', 'province_of_employment', 
                     'date_employed', 'date_verified', 'verification_status', 'monthly_salary', 'monthly_salary_indicator',
                     'date_left', 'segment_code', 'segment_description', 'order_in_segment']
        )
        empl_scheme = [
            SchemaField('bus_ptnr', 'STRING'),
            SchemaField('file_date', 'DATE'),
            SchemaField('occupation', 'STRING'),
            SchemaField('city_of_employment', 'STRING'),
            SchemaField('province_of_employment', 'STRING'),
            SchemaField('date_employed', 'STRING'),
            SchemaField('date_verified', 'STRING'),
            SchemaField('verification_status', 'STRING'),
            SchemaField('monthly_salary', 'NUMERIC'),
            SchemaField('monthly_salary_indicator', 'STRING'),
            SchemaField('date_left', 'STRING'),
            SchemaField('segment_code', 'STRING'),
            SchemaField('segment_description', 'STRING'),
            SchemaField('order_in_segment', 'INT64')
        ]
        self.data['idx_ES'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("ES ", x)])
        self.data['idx_EF'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" EF ", x)])
        self.data['idx_E2'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" E2 ", x)])
        count = 0
        for ncol in self.data.index:
            bp, dt, dt_str, mfile = self._parse_entry_details(ncol=ncol)
            es_index, es_records, ef_index, ef_records, e2_index, e2_records = self._parse_seg_index(ncol=ncol, seg_list = ['idx_ES', 'idx_EF', 'idx_E2'])
            if es_records + ef_records + e2_records > 0:
                if es_records != 0:
                    count_es = 1
                    for i in es_index:
                        segment = mfile[i:]
                        self.empl.loc[count, 'bus_ptnr'] = bp
                        self.empl.loc[count, 'file_date'] = dt
                        self.empl.loc[count, 'occupation'] = segment[3:37]
                        self.empl.loc[count, 'employer'] = segment[38:72]
                        self.empl.loc[count, 'city_of_employment'] = segment[80:88]
                        self.empl.loc[count, 'province_of_employment'] = segment[89:91]
                        self.empl.loc[count, 'date_employed'] = segment[95:99] + segment[92:94]
                        self.empl.loc[count, 'date_verified'] = segment[103:107] + segment[100:102]
                        self.empl.loc[count, 'verification_status'] = segment[108:109]
                        self.empl.loc[count, 'monthly_salary'] = segment[110:118]
                        self.empl.loc[count, 'date_left'] = segment[122:126] + segment[119:121]
                        self.empl.loc[count, 'segment_code'] = 'ES'
                        self.empl.loc[count, 'segment_description'] = 'current employment situation'
                        self.empl.loc[count, 'order_in_segment'] = count_es
                        count += 1
                        count_es += 1
                if ef_records != 0:
                    count_ef = 1
                    for i in ef_index:
                        segment = mfile[i:]
                        self.empl.loc[count, 'bus_ptnr'] = bp
                        self.empl.loc[count, 'file_date'] = dt
                        self.empl.loc[count, 'occupation'] = segment[3:37]
                        self.empl.loc[count, 'employer'] = segment[38:72]
                        self.empl.loc[count, 'city_of_employment'] = segment[80:88]
                        self.empl.loc[count, 'province_of_employment'] = segment[89:91]
                        self.empl.loc[count, 'date_employed'] = segment[95:99] + segment[92:94]
                        self.empl.loc[count, 'date_verified'] = segment[103:107] + segment[100:102]
                        self.empl.loc[count, 'verification_status'] = segment[108:109]
                        self.empl.loc[count, 'monthly_salary'] = segment[110:118]
                        self.empl.loc[count, 'date_left'] = segment[122:126] + segment[119:121]
                        self.empl.loc[count, 'segment_code'] = 'EF'
                        self.empl.loc[count, 'segment_description'] = 'former employment situation'
                        self.empl.loc[count, 'order_in_segment'] = count_ef
                        count += 1
                        count_ef += 1
                if e2_records != 0:
                    count_e2 = 1
                    for i in e2_index:
                        segment = mfile[i:]
                        self.empl.loc[count, 'bus_ptnr'] = bp
                        self.empl.loc[count, 'file_date'] = dt
                        self.empl.loc[count, 'occupation'] = segment[3:37]
                        self.empl.loc[count, 'employer'] = segment[38:72]
                        self.empl.loc[count, 'city_of_employment'] = segment[80:88]
                        self.empl.loc[count, 'province_of_employment'] = segment[89:91]
                        self.empl.loc[count, 'date_employed'] = segment[95:99] + segment[92:94]
                        self.empl.loc[count, 'date_verified'] = segment[103:107] + segment[100:102]
                        self.empl.loc[count, 'verification_status'] = segment[108:109]
                        self.empl.loc[count, 'monthly_salary'] = segment[110:118]
                        self.empl.loc[count, 'date_left'] = segment[122:126] + segment[119:121]
                        self.empl.loc[count, 'segment_code'] = 'E2'
                        self.empl.loc[count, 'segment_description'] = 'former employment situation'
                        self.empl.loc[count, 'order_in_segment'] = count_e2
                        count += 1
                        count_e2 += 1
        self.column_taboo += ['idx_ES', 'idx_EF', 'idx_E2']
        self.empl['check1'] = self.empl.date_employed.apply(FilterAndConverter.filter_6digits_date)
        self.empl['check2'] = self.empl.date_verified.apply(FilterAndConverter.filter_6digits_date)
        self.empl['check3'] = self.empl.date_left.apply(FilterAndConverter.filter_6digits_date)
        self.empl['check4'] = self.empl.city_of_employment.apply(lambda x: x.strip().isalpha() if x.strip() else True)
        self.empl['check5'] = self.empl.province_of_employment.apply(lambda x: x.strip().isalpha() if x.strip() else True)
        self.empl['check6'] = self.empl.monthly_salary.apply(lambda x: '$' in x if x.strip() else False)
        self.empl = self.empl.loc[self.empl.check1 & self.empl.check2 & self.empl.check3 & self.empl.check4 & self.empl.check5 & self.empl.check6]
        self.empl.drop(columns = ['check1', 'check2', 'check3', 'check4', 'check5', 'check6'], inplace=True)
        self.empl['monthly_salary_indicator'] = self.empl.monthly_salary.apply(lambda x: 'NV' if x[-2:] == 'NV' else '')
        self.empl['monthly_salary'] = self.empl.monthly_salary.apply(lambda x: x[:-2] if x[-2:] == 'NV' else x)
        self.empl['monthly_salary'] = self.empl.monthly_salary.apply(FilterAndConverter.convert_amount_from_str).astype('Int64')
        self._push_seg_table(table=self.empl, table_len=len(self.empl), seg_name='7_8_9_employment', schema=empl_scheme)           
        
    # 5. parsing other income    
    def _parse_other_income(self):
        self.oinc = pd.DataFrame(
            columns=['bus_ptnr', 'file_date', 'date_reported', 'income_amount', 'income_source', 
                     'date_verified', 'verification_status', 'segment_code', 'segment_description', 'order_in_segment']
        )
        oinc_scheme = [
            SchemaField('bus_ptnr', 'STRING'),
            SchemaField('file_date', 'DATE'),
            SchemaField('date_reported', 'STRING'),
            SchemaField('income_amount', 'NUMERIC'),
            SchemaField('income_source', 'STRING'),
            SchemaField('date_verified', 'STRING'),
            SchemaField('verification_status', 'STRING'),
            SchemaField('segment_code', 'STRING'),
            SchemaField('segment_description', 'STRING'),
            SchemaField('order_in_segment', 'INT64')
        ]
        self.data['idx_OI'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" OI ", x)])
        count = 0
        for ncol in self.data.index:            
            bp, dt, dt_str, mfile = self._parse_entry_details(ncol=ncol)
            oi_index, oi_records = self._parse_seg_index(ncol=ncol, seg_list = ['idx_OI'])
            if oi_records != 0:
                count_oi = 1
                for i in oi_index:
                    segment = mfile[i:]
                    self.oinc.loc[count, 'bus_ptnr'] = bp
                    self.oinc.loc[count, 'file_date'] = dt
                    self.oinc.loc[count, 'date_reported'] = segment[6:10] + segment[3:5]
                    self.oinc.loc[count, 'income_amount'] = segment[11:17]
                    self.oinc.loc[count, 'income_source'] = segment[18:58]
                    self.oinc.loc[count, 'date_verified'] = segment[62:66] + segment[59:61]
                    self.oinc.loc[count, 'verification_status'] = segment[67:68]
                    self.oinc.loc[count, 'segment_code'] = 'OI'
                    self.oinc.loc[count, 'segment_description'] = 'other income'
                    self.oinc.loc[count, 'order_in_segment'] = count_oi
                    count += 1
                    count_oi += 1
        self.column_taboo.append('idx_OI')
        self.oinc['check1'] = self.oinc.date_reported.apply(FilterAndConverter.filter_6digits_date)
        self.oinc['check2'] = self.oinc.date_verified.apply(FilterAndConverter.filter_6digits_date)
        self.oinc = self.oinc.loc[self.oinc.check1 & self.oinc.check2]
        self.oinc.drop(columns = ['check1', 'check2'], inplace=True)
        self.oinc['income_amount'] = self.oinc.income_amount.apply(FilterAndConverter.convert_amount_from_str).astype('Int64')
        self._push_seg_table(table=self.oinc, table_len=len(self.oinc), seg_name='12_other_income', schema=oinc_scheme)      
     
    # 6. parsing bankruptcy
    def _parse_bankruptcy(self):
        self.bkpt = pd.DataFrame(
            columns=['bus_ptnr', 'file_date', 'foreign_bureau_code', 'date_filed', 'name_court', 'court_number',
                     'type_bankruptcy', 'how_filed', 'deposition_codes', 'amount_liability', 'asset_amount', 'date_settled',
                     'narrative_code_1', 'narrative_code_2', 'case_number', 'segment_code', 'segment_description','order_in_segment']
        )
        bkpt_scheme = [
            SchemaField('bus_ptnr', 'STRING'),
            SchemaField('file_date', 'DATE'),
            SchemaField('foreign_bureau_code', 'STRING'),
            SchemaField('date_filed', 'STRING'),
            SchemaField('name_court', 'STRING'),
            SchemaField('court_number', 'STRING'),
            SchemaField('type_bankruptcy', 'STRING'),
            SchemaField('how_filed', 'STRING'),
            SchemaField('deposition_codes', 'STRING'),
            SchemaField('amount_liability', 'NUMERIC'),
            SchemaField('asset_amount', 'NUMERIC'),
            SchemaField('date_settled', 'STRING'),
            SchemaField('narrative_code_1', 'STRING'),
            SchemaField('narrative_code_2', 'STRING'),
            SchemaField('case_number', 'STRING'),
            SchemaField('segment_code', 'STRING'),
            SchemaField('segment_description', 'STRING'),
            SchemaField('order_in_segment', 'INT64')
        ]
        self.data['idx_BP'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("BP ", x)])
        count = 0
        for ncol in self.data.index:
            bp, dt, dt_str, mfile = self._parse_entry_details(ncol=ncol)
            bp_index, bp_records = self._parse_seg_index(ncol=ncol, seg_list = ['idx_BP'])
            if bp_records != 0:
                count_bp = 1
                for i in bp_index:
                    segment = mfile[i:]
                    self.bkpt.loc[count, 'bus_ptnr'] = bp
                    self.bkpt.loc[count, 'file_date'] = dt
                    self.bkpt.loc[count, 'foreign_bureau_code'] = segment[3:4]
                    self.bkpt.loc[count, 'date_filed'] = segment[8:12] + segment[5:7]
                    self.bkpt.loc[count, 'name_court'] = segment[13:33]
                    self.bkpt.loc[count, 'court_number'] = segment[52:62]
                    self.bkpt.loc[count, 'type_bankruptcy'] = segment[63:64]
                    self.bkpt.loc[count, 'how_filed'] = segment[65:66]
                    self.bkpt.loc[count, 'deposition_codes'] = segment[67:68]
                    self.bkpt.loc[count, 'amount_liability'] = segment[69:75]
                    self.bkpt.loc[count, 'asset_amount'] = segment[80:86]
                    self.bkpt.loc[count, 'date_settled'] = segment[90:94] + segment[87:89]
                    self.bkpt.loc[count, 'narrative_code_1'] = segment[95:97]
                    self.bkpt.loc[count, 'narrative_code_2'] = segment[98:100]
                    self.bkpt.loc[count, 'case_number'] = segment[101:143]
                    self.bkpt.loc[count, 'segment_code'] = 'BP'
                    self.bkpt.loc[count, 'segment_description'] = 'bankruptcy'
                    self.bkpt.loc[count, 'order_in_segment'] = count_bp
                    count += 1
                    count_bp += 1
        self.column_taboo.append('idx_BP')
        self.bkpt['check1'] = self.bkpt.how_filed.apply(lambda x: x in ['S', 'J', ' '])
        self.bkpt['check2'] = self.bkpt.type_bankruptcy.apply(lambda x: x in ['B', 'I', ' '])
        self.bkpt['check3'] = self.bkpt.case_number.apply(FilterAndConverter.filter_first_not_null)
        self.bkpt['check4'] = self.bkpt.date_filed.apply(FilterAndConverter.filter_6digits_date)
        self.bkpt['check5'] = self.bkpt.date_settled.apply(FilterAndConverter.filter_6digits_date)
        self.bkpt['check6'] = self.bkpt.narrative_code_1.apply(FilterAndConverter.filter_industry_code)
        self.bkpt['check7'] = self.bkpt.narrative_code_2.apply(FilterAndConverter.filter_industry_code)
        self.bkpt = self.bkpt.loc[
            self.bkpt.check1 & self.bkpt.check2 & self.bkpt.check3 & self.bkpt.check4 & self.bkpt.check5 & self.bkpt.check6 & self.bkpt.check7
        ]
        self.bkpt.drop(columns = ['check1', 'check2', 'check3', 'check4', 'check5', 'check6', 'check7'], inplace=True)
        self.bkpt['amount_liability'] = self.bkpt.amount_liability.apply(FilterAndConverter.convert_amount_from_str).astype('Int64')
        self.bkpt['asset_amount'] = self.bkpt.asset_amount.apply(FilterAndConverter.convert_amount_from_str).astype('Int64')
        self._push_seg_table(table=self.bkpt, table_len=len(self.bkpt), seg_name='13_bankruptcy', schema=bkpt_scheme) 

    # 7. parsing collection    
    def _parse_collection(self):
        self.colt = pd.DataFrame(
            columns=['bus_ptnr', 'file_date', 'foreign_bureau_code', 'date_reported', 'name_member', 'member_number', 'amount', 
                     'balance', 'type', 'narrative_code_1', 'narrative_code_2', 'industry_code', 'reason_code', 'date_paid', 
                     'date_last_payment', 'creditors_account_number_and_name', 'ledger_number', 'segment_code', 'segment_description', 
                     'order_in_segment']
        )
        colt_scheme = [
            SchemaField('bus_ptnr', 'STRING'),
            SchemaField('file_date', 'DATE'),
            SchemaField('foreign_bureau_code', 'STRING'),
            SchemaField('date_reported', 'STRING'),
            SchemaField('name_member', 'STRING'),
            SchemaField('member_number', 'STRING'),
            SchemaField('amount', 'NUMERIC'),
            SchemaField('balance', 'NUMERIC'),
            SchemaField('type', 'STRING'),
            SchemaField('narrative_code_1', 'STRING'),
            SchemaField('narrative_code_2', 'STRING'),
            SchemaField('industry_code', 'STRING'),
            SchemaField('reason_code', 'STRING'),
            SchemaField('date_paid', 'STRING'),
            SchemaField('date_last_payment', 'STRING'),
            SchemaField('creditors_account_number_and_name', 'STRING'),
            SchemaField('ledger_number', 'STRING'),
            SchemaField('segment_code', 'STRING'),
            SchemaField('segment_description', 'STRING'),
            SchemaField('order_in_segment', 'INT64')
        ]
        self.data['idx_CO'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" CO ", x)])
        count = 0
        for ncol in self.data.index:
            bp, dt, dt_str, mfile = self._parse_entry_details(ncol=ncol)
            co_index, co_records = self._parse_seg_index(ncol=ncol, seg_list = ['idx_CO'])
            if co_records != 0:
                flag = ''
                count_co = 1
                for i in co_index:
                    segment = mfile[i:]
                    self.colt.loc[count, 'bus_ptnr'] = bp
                    self.colt.loc[count, 'file_date'] = dt
                    self.colt.loc[count, 'foreign_bureau_code'] = segment[3:4]
                    self.colt.loc[count, 'date_reported'] = segment[8:12] + segment[5:7]
                    self.colt.loc[count, 'name_member'] = segment[13:33]
                    self.colt.loc[count, 'member_number'] = segment[52:62]
                    self.colt.loc[count, 'amount'] = segment[63:69]
                    self.colt.loc[count, 'balance'] = segment[70:76]
                    self.colt.loc[count, 'type'] = segment[77:78]
                    self.colt.loc[count, 'narrative_code_1'] = segment[80:82]
                    self.colt.loc[count, 'narrative_code_2'] = segment[83:85]
                    self.colt.loc[count, 'industry_code'] = segment[86:88]
                    self.colt.loc[count, 'reason_code'] = segment[89:90]
                    self.colt.loc[count, 'date_paid'] = segment[94:98] + segment[91:93]
                    self.colt.loc[count, 'date_last_payment'] = segment[102:106] + segment[99:101]
                    self.colt.loc[count, 'creditors_account_number_and_name'] = segment[107:157]
                    self.colt.loc[count, 'ledger_number'] = segment[160:177]
                    self.colt.loc[count, 'segment_code'] = 'CO'
                    self.colt.loc[count, 'segment_description'] = 'collection'
                    self.colt.loc[count, 'order_in_segment'] = count_co
                    count += 1
                    count_co += 1
        self.column_taboo.append('idx_CO')
        self.colt['check1'] = self.colt.type.apply(lambda x: x in ['P', 'U', ' '])
        self.colt['check2'] = self.colt.date_reported.apply(FilterAndConverter.filter_6digits_date)
        self.colt['check3'] = self.colt.date_paid.apply(FilterAndConverter.filter_6digits_date)
        self.colt['check4'] = self.colt.date_last_payment.apply(FilterAndConverter.filter_6digits_date)
        self.colt['check5'] = self.colt.member_number.apply(FilterAndConverter.filter_member_number)
        self.colt['check6'] = self.colt.creditors_account_number_and_name.apply(FilterAndConverter.filter_first_not_null)
        self.colt['check7'] = self.colt.narrative_code_1.apply(FilterAndConverter.filter_industry_code)
        self.colt['check8'] = self.colt.narrative_code_2.apply(FilterAndConverter.filter_industry_code)
        self.colt = self.colt.loc[
            self.colt.check1 & self.colt.check2 & self.colt.check3 & self.colt.check4 & self.colt.check5 & self.colt.check6 & self.colt.check7 & self.colt.check8
        ]
        self.colt.drop(columns = ['check1', 'check2', 'check3', 'check4', 'check5', 'check6', 'check7', 'check8'], inplace=True)
        self.colt['amount'] = self.colt.amount.apply(FilterAndConverter.convert_amount_from_str).astype('Int64')
        self.colt['balance'] = self.colt.balance.apply(FilterAndConverter.convert_amount_from_str).astype('Int64')
        self._push_seg_table(table=self.colt, table_len=len(self.colt), seg_name='14_collection', schema=colt_scheme)     

    # 8. parsing secured loan
    def _parse_secured_loan(self):
        self.selo = pd.DataFrame(
            columns=['bus_ptnr', 'file_date', 'foreign_bureau_code', 'date_filed', 'name_court', 'court_number', 'industry_code', 
                     'maturity_date', 'narrative_code_1', 'narrative_code_2', 'creditors_name_address_amount', 'segment_code',
                     'segment_description', 'order_in_segment']
        )
        selo_scheme = [
            SchemaField('bus_ptnr', 'STRING'),
            SchemaField('file_date', 'DATE'),
            SchemaField('foreign_bureau_code', 'STRING'),
            SchemaField('date_filed', 'STRING'),
            SchemaField('name_court', 'STRING'),
            SchemaField('court_number', 'STRING'),
            SchemaField('industry_code', 'STRING'),
            SchemaField('maturity_date', 'STRING'),
            SchemaField('narrative_code_1', 'STRING'),
            SchemaField('narrative_code_2', 'STRING'),
            SchemaField('creditors_name_address_amount', 'STRING'),
            SchemaField('segment_code', 'STRING'),
            SchemaField('segment_description', 'STRING'),
            SchemaField('order_in_segment', 'INT64')
        ]
        self.data['idx_FM'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("FM ", x)])
        count = 0
        for ncol in self.data.index:
            bp, dt, dt_str, mfile = self._parse_entry_details(ncol=ncol)
            fm_index, fm_records = self._parse_seg_index(ncol=ncol, seg_list = ['idx_FM'])
            if fm_records != 0:
                count_fm = 1
                for i in fm_index:
                    segment = mfile[i:]
                    self.selo.loc[count, 'bus_ptnr'] = bp
                    self.selo.loc[count, 'file_date'] = dt
                    self.selo.loc[count, 'foreign_bureau_code'] = segment[3:4]
                    self.selo.loc[count, 'date_filed'] = segment[8:12] + segment[5:7]
                    self.selo.loc[count, 'name_court'] = segment[13:33]
                    self.selo.loc[count, 'court_number'] = segment[52:62]
                    self.selo.loc[count, 'industry_code'] = segment[63:65]
                    self.selo.loc[count, 'maturity_date'] = segment[69:73] + segment[66:68]
                    self.selo.loc[count, 'narrative_code_1'] = segment[73:76]
                    self.selo.loc[count, 'narrative_code_2'] = segment[77:79]
                    self.selo.loc[count, 'creditors_name_address_amount'] = segment[80:140].lstrip()
                    self.selo.loc[count, 'segment_code'] = 'FM'
                    self.selo.loc[count, 'segment_description'] = 'secured loan'
                    self.selo.loc[count, 'order_in_segment'] = count_fm
                    count += 1
                    count_fm += 1
        self.column_taboo.append('idx_FM')
        self.selo['check1'] = self.selo.industry_code.apply(FilterAndConverter.filter_industry_code)
        self.selo['check2'] = self.selo.date_filed.apply(FilterAndConverter.filter_6digits_date)
        self.selo['check3'] = self.selo.maturity_date.apply(FilterAndConverter.filter_6digits_date)
        self.selo['check4'] = self.selo.narrative_code_1.apply(FilterAndConverter.filter_industry_code)
        self.selo['check5'] = self.selo.narrative_code_2.apply(FilterAndConverter.filter_industry_code)
        self.selo['check6'] = self.selo.creditors_name_address_amount.apply(lambda x: x[0].isdigit() if x.strip() != '' else True)
        self.selo = self.selo.loc[self.selo.check1 & self.selo.check2 & self.selo.check3 & self.selo.check4 & self.selo.check5 & self.selo.check6]
        self.selo.drop(columns = ['check1', 'check2', 'check3', 'check4', 'check5', 'check6'], inplace=True)
        self._push_seg_table(table=self.selo, table_len=len(self.selo), seg_name='15_secured_loan', schema=selo_scheme)      
    
    # 9. parsing legal item    
    def _parse_legal_item(self):
        self.leit = pd.DataFrame(
            columns=['bus_ptnr', 'file_date', 'foreign_bureau_code', 'date_filed', 'name_court', 'court_number', 'amount', 'type_code', 
                     'date_satisfied', 'status_code', 'date_verified', 'narrative_code_1', 'narrative_code_2', 'defendant', 'case_number', 
                     'case_number_continued', 'plaintiff', 'laywer_name_address', 'segment_code', 'segment_description', 'order_in_segment']
        )
        leit_scheme = [
            SchemaField('bus_ptnr', 'STRING'),
            SchemaField('file_date', 'DATE'),
            SchemaField('foreign_bureau_code', 'STRING'),
            SchemaField('date_filed', 'STRING'),
            SchemaField('name_court', 'STRING'),
            SchemaField('court_number', 'STRING'),
            # if set to NUMERIC an error pops (Got bytestring of length 8 (expected 16)) out for 2020.1 data; but OKAY for INT64
            SchemaField('amount', 'INT64'),   
            SchemaField('type_code', 'STRING'),
            SchemaField('date_satisfied', 'STRING'),
            SchemaField('status_code', 'STRING'),
            SchemaField('date_verified', 'STRING'),
            SchemaField('narrative_code_1', 'STRING'),
            SchemaField('narrative_code_2', 'STRING'),
            SchemaField('defendant', 'STRING'),
            SchemaField('case_number', 'STRING'),
            SchemaField('case_number_continued', 'STRING'),
            SchemaField('plaintiff', 'STRING'),
            SchemaField('laywer_name_address', 'STRING'),
            SchemaField('segment_code', 'STRING'),
            SchemaField('segment_description', 'STRING'),
            SchemaField('order_in_segment', 'INT64')
        ]
        self.data['idx_LI'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("LI ", x)])
        count = 0
        for ncol in self.data.index:
            bp, dt, dt_str, mfile = self._parse_entry_details(ncol=ncol)
            li_index, li_records = self._parse_seg_index(ncol=ncol, seg_list = ['idx_LI'])
            if li_records != 0:
                count_li = 1
                for i in li_index:
                    segment = mfile[i:]
                    self.leit.loc[count, 'bus_ptnr'] = bp
                    self.leit.loc[count, 'file_date'] = dt
                    self.leit.loc[count, 'foreign_bureau_code'] = segment[3:4]
                    self.leit.loc[count, 'date_filed'] = segment[8:12] + segment[5:7]
                    self.leit.loc[count, 'name_court'] = segment[13:33]
                    self.leit.loc[count, 'court_number'] = segment[52:62]
                    self.leit.loc[count, 'amount'] = segment[63:69]
                    self.leit.loc[count, 'type_code'] = segment[70:71]
                    self.leit.loc[count, 'date_satisfied'] = segment[75:79] + segment[72:74]
                    self.leit.loc[count, 'status_code'] = segment[80:81]
                    self.leit.loc[count, 'date_verified'] = segment[82:89]
                    self.leit.loc[count, 'narrative_code_1'] = segment[90:92]
                    self.leit.loc[count, 'narrative_code_2'] = segment[93:95]
                    self.leit.loc[count, 'defendant'] = segment[96:136]
                    self.leit.loc[count, 'case_number'] = segment[137:159]
                    self.leit.loc[count, 'case_number_continued'] = segment[160:180]
                    self.leit.loc[count, 'plaintiff'] = segment[181:221]
                    self.leit.loc[count, 'laywer_name_address'] = segment[240:300]
                    self.leit.loc[count, 'segment_code'] = 'LI'
                    self.leit.loc[count, 'segment_description'] = 'legal item'
                    self.leit.loc[count, 'order_in_segment'] = count_li
                    count += 1
                    count_li += 1
        self.column_taboo.append('idx_LI')
        self.leit['check1'] = self.leit.type_code.apply(lambda x: x in ['A', 'J', 'F'])
        self.leit['check2'] = self.leit.status_code.apply(lambda x: x in ['D', 'S', 'T'])
        self.leit['check3'] = self.leit.amount.apply(lambda x: '\\' not in x)
        self.leit['check4'] = self.leit.name_court.apply(lambda x: x[0] != ' ')
        self.leit['check5'] = self.leit.date_filed.apply(FilterAndConverter.filter_6digits_date)
        self.leit['check6'] = self.leit.date_satisfied.apply(FilterAndConverter.filter_6digits_date)
        self.leit['check7'] = self.leit.narrative_code_1.apply(FilterAndConverter.filter_industry_code)
        self.leit['check8'] = self.leit.narrative_code_2.apply(FilterAndConverter.filter_industry_code)
        self.leit = self.leit.loc[
            (self.leit.check1 | self.leit.check2) & self.leit.check3 & self.leit.check4 & self.leit.check5 & self.leit.check6 & self.leit.check7 & self.leit.check8
        ]
        self.leit.drop(columns = ['check1', 'check2', 'check3', 'check4', 'check5', 'check6', 'check7', 'check8'], inplace=True)
        self.leit['amount'] = self.leit.amount.apply(FilterAndConverter.convert_amount_from_str).astype('Int64')
        self._push_seg_table(table=self.leit, table_len=len(self.leit), seg_name='16_legal_item', schema=leit_scheme)      
    
    # 10. parsing foreclosure (FO), discontinued
    # def _parse_foreclosure(self):
    #     self.focl = pd.DataFrame(
    #         columns=['bus_ptnr', 'file_date', 'foreign_bureau_code', 'date_reported', 'date_checked', 'narrative_code_1', 
    #                  'narrative_code_2', 'member_number_or_member_narrative', 'segment_code', 'segment_description', 'order_in_segment']
    #     )
    #     focl_scheme = [
    #         SchemaField('bus_ptnr', 'STRING'),
    #         SchemaField('file_date', 'DATE'),
    #         SchemaField('foreign_bureau_code', 'STRING'),
    #         SchemaField('date_reported', 'STRING'),
    #         SchemaField('date_checked', 'STRING'),
    #         SchemaField('narrative_code_1', 'STRING'),
    #         SchemaField('narrative_code_2', 'STRING'),
    #         SchemaField('member_number_or_member_narrative', 'STRING'),
    #         SchemaField('segment_code', 'STRING'),
    #         SchemaField('segment_description', 'STRING'),
    #         SchemaField('order_in_segment', 'INT64')
    #     ]
    #     self.data['idx_FO'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" FO ", x)])
    #     count = 0
    #     for ncol in self.data.index:
    #         bp, dt, dt_str, mfile = self._parse_entry_details(ncol=ncol)
    #         fo_index, fo_records = self._parse_seg_index(ncol=ncol, seg_list = ['idx_FO'])
    #         if fo_records != 0:
    #             count_fo = 1
    #             for i in fo_index:
    #                 segment = mfile[i:]
    #                 self.focl.loc[count, 'bus_ptnr'] = bp
    #                 self.focl.loc[count, 'file_date'] = dt
    #                 self.focl.loc[count, 'foreign_bureau_code'] = segment[3:4]
    #                 self.focl.loc[count, 'date_reported'] = segment[8:12] + segment[5:7]
    #                 self.focl.loc[count, 'date_checked'] = segment[16:20] + segment[13:15]
    #                 self.focl.loc[count, 'narrative_code_1'] = segment[21:23]
    #                 self.focl.loc[count, 'narrative_code_2'] = segment[24:26]
    #                 self.focl.loc[count, 'member_number_or_member_narrative'] = segment[27:67]
    #                 self.focl.loc[count, 'segment_code'] = 'FO'
    #                 self.focl.loc[count, 'segment_description'] = 'foreclosure'
    #                 self.focl.loc[count, 'order_in_segment'] = count_fo
    #                 count += 1
    #                 count_fo += 1
    #     self.column_taboo.append('idx_FO')
    #     self.focl['check1'] = self.focl.date_reported.apply(FilterAndConverter.filter_6digits_date)
    #     self.focl['check2'] = self.focl.date_checked.apply(FilterAndConverter.filter_6digits_date)
    #     self.focl['check3'] = self.focl.narrative_code_1.apply(FilterAndConverter.filter_industry_code)
    #     self.focl['check4'] = self.focl.narrative_code_2.apply(FilterAndConverter.filter_industry_code)
    #     self.focl = self.focl.loc[self.focl.check1 & self.focl.check2 & self.focl.check3 & self.focl.check4]
    #     self.focl.drop(columns = ['check1', 'check2', 'check3', 'check4'], inplace=True)
    #     self._push_seg_table(table=self.focl, table_len=len(self.focl), seg_name='17_foreclosure', schema=focl_scheme)
     
    # 11. parsing non-responsibility (NR), discountinued  
    # def _parse_non_responsibility(self):
    #     self.nres = pd.DataFrame(
    #         columns=['bus_ptnr', 'file_date', 'foreign_bureau_code', 'date_reported', 'person_filling', 'narrative_code_1', 
    #                  'narrative_code_2', 'segment_code', 'segment_description', 'order_in_segment']
    #     )
    #     nres_scheme = [
    #         SchemaField('bus_ptnr', 'STRING'),
    #         SchemaField('file_date', 'DATE'),
    #         SchemaField('foreign_bureau_code', 'STRING'),
    #         SchemaField('date_reported', 'STRING'),
    #         SchemaField('person_filling', 'STRING'),
    #         SchemaField('narrative_code_1', 'STRING'),
    #         SchemaField('narrative_code_2', 'STRING'),
    #         SchemaField('segment_code', 'STRING'),
    #         SchemaField('segment_description', 'STRING'),
    #         SchemaField('order_in_segment', 'INT64')
    #     ]
    #     self.data['idx_NR'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" NR ", x)])
    #     count = 0
    #     for ncol in self.data.index:
    #         bp, dt, dt_str, mfile = self._parse_entry_details(ncol=ncol)
    #         nr_index, nr_records = self._parse_seg_index(ncol=ncol, seg_list = ['idx_NR'])
    #         if nr_records != 0:
    #             count_nr = 1
    #             for i in nr_index:
    #                 segment = mfile[i:]
    #                 self.nres.loc[count, 'bus_ptnr'] = bp
    #                 self.nres.loc[count, 'file_date'] = dt
    #                 self.nres.loc[count, 'foreign_bureau_code'] = segment[3:4]
    #                 self.nres.loc[count, 'date_reported'] = segment[8:12] + segment[5:7]
    #                 self.nres.loc[count, 'person_filling'] = segment[13:14]
    #                 self.nres.loc[count, 'narrative_code_1'] = segment[15:17]
    #                 self.nres.loc[count, 'narrative_code_2'] = segment[18:20]
    #                 self.nres.loc[count, 'segment_code'] = 'NR'
    #                 self.nres.loc[count, 'segment_description'] = 'non-responsibility'
    #                 self.nres.loc[count, 'order_in_segment'] = count_nr
    #                 count += 1
    #                 count_nr += 1
    #     self.column_taboo.append('idx_NR')
    #     self.nres['check1'] = self.nres.date_reported.apply(FilterAndConverter.filter_6digits_date)
    #     self.nres['check2'] = self.nres.person_filling.apply(lambda x: x in ['S', 'W', 'B'])
    #     self.nres['check3'] = self.nres.narrative_code_1.apply(FilterAndConverter.filter_industry_code)
    #     self.nres['check4'] = self.nres.narrative_code_2.apply(FilterAndConverter.filter_industry_code)
    #     self.nres = self.nres.loc[self.nres.check1 & self.nres.check2 & self.nres.check3 & self.nres.check4]
    #     self.nres.drop(columns = ['check1', 'check2', 'check3','check4'], inplace=True)
    #     self._push_seg_table(table=self.nres, table_len=len(self.nres), seg_name='18_non_responsibility', schema=nres_scheme)  
     
    # 12. parsing marital item
    def _parse_marital_item(self):
        self.mari = pd.DataFrame(
            columns=['bus_ptnr', 'file_date', 'foreign_bureau_code', 'date_reported', 'name_court', 'telephone_area_code',
                     'telephone_number', 'extension', 'member_number', 'action_code', 'date_verified', 'amount', 'additional_details', 
                     'segment_code', 'segment_description', 'order_in_segment']
        )
        mari_scheme = [
            SchemaField('bus_ptnr', 'STRING'),
            SchemaField('file_date', 'DATE'),
            SchemaField('foreign_bureau_code', 'STRING'),
            SchemaField('date_reported', 'STRING'),
            SchemaField('name_court', 'STRING'),
            SchemaField('telephone_area_code', 'STRING'),
            SchemaField('telephone_number', 'STRING'),
            SchemaField('extension', 'STRING'),
            SchemaField('member_number', 'STRING'),
            SchemaField('action_code', 'STRING'),
            SchemaField('date_verified', 'STRING'),
            SchemaField('amount', 'STRING'),
            SchemaField('additional_details', 'STRING'),
            SchemaField('segment_code', 'STRING'),
            SchemaField('segment_description', 'STRING'),
            SchemaField('order_in_segment', 'INT64')
        ]
        self.data['idx_MI'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" MI ", x)])
        count = 0
        for ncol in self.data.index:
            bp, dt, dt_str, mfile = self._parse_entry_details(ncol=ncol)
            mi_index, mi_records = self._parse_seg_index(ncol=ncol, seg_list = ['idx_MI'])
            if mi_records != 0:
                count_mi = 1
                for i in mi_index:
                    segment = mfile[i:]
                    self.mari.loc[count, 'bus_ptnr'] = bp
                    self.mari.loc[count, 'file_date'] = dt
                    self.mari.loc[count, 'foreign_bureau_code'] = segment[3:4]
                    self.mari.loc[count, 'date_reported'] = segment[8:12] + segment[5:7]
                    self.mari.loc[count, 'name_court'] = segment[13:33]
                    self.mari.loc[count, 'telephone_area_code'] = segment[34:37]
                    self.mari.loc[count, 'telephone_number'] = segment[38:46]
                    self.mari.loc[count, 'extension'] = segment[47:51]
                    self.mari.loc[count, 'member_number'] = segment[52:62]
                    self.mari.loc[count, 'action_code'] = segment[63:64]
                    self.mari.loc[count, 'date_verified'] = segment[68:72] + segment[65:67]
                    self.mari.loc[count, 'amount'] = segment[80:122]
                    self.mari.loc[count, 'additional_details'] = segment[160:200]
                    self.mari.loc[count, 'segment_code'] = 'MI'
                    self.mari.loc[count, 'segment_description'] = 'marital item'
                    self.mari.loc[count, 'order_in_segment'] = count_mi
                    count += 1
                    count_mi += 1
        self.column_taboo.append('idx_MI')
        self.mari['check1'] = self.mari.date_reported.apply(FilterAndConverter.filter_6digits_date)
        self.mari['check2'] = self.mari.date_verified.apply(FilterAndConverter.filter_6digits_date)
        self.mari['check3'] = self.mari.member_number.apply(FilterAndConverter.filter_member_number)
        self.mari['check4'] = self.mari.action_code.apply(lambda x: x in ['S', ' '])
        self.mari = self.mari.loc[self.mari.check1 & self.mari.check2 & self.mari.check3 & self.mari.check4]
        self.mari.drop(columns = ['check1', 'check2', 'check3', 'check4'], inplace=True)
        self._push_seg_table(table=self.mari, table_len=len(self.mari), seg_name='19_marital_item', schema=mari_scheme)
   
    # 13. parsing tax lien (TL), discountinued
    # def _parse_tax_lien(self):
    #     self.tali = pd.DataFrame(
    #         columns=['bus_ptnr', 'file_date', 'foreign_bureau_code', 'date_filed', 'name_court', 'court_number', 'amount', 
    #                  'industry_code', 'date_released', 'date_verified', 'narrative_code_1', 'narrative_code_2', 'case_number', 
    #                  'segment_code', 'segment_description', 'order_in_segment']
    #     )
    #     tali_scheme = [
    #         SchemaField('bus_ptnr', 'STRING'),
    #         SchemaField('file_date', 'DATE'),
    #         SchemaField('foreign_bureau_code', 'STRING'),
    #         SchemaField('date_filed', 'STRING'),
    #         SchemaField('name_court', 'STRING'),
    #         SchemaField('court_number', 'STRING'),
    #         SchemaField('amount', 'NUMERIC'),
    #         SchemaField('industry_code', 'STRING'),
    #         SchemaField('date_released', 'STRING'),
    #         SchemaField('date_verified', 'STRING'),
    #         SchemaField('narrative_code_1', 'STRING'),
    #         SchemaField('narrative_code_2', 'STRING'),
    #         SchemaField('case_number', 'STRING'),
    #         SchemaField('segment_code', 'STRING'),
    #         SchemaField('segment_description', 'STRING'),
    #         SchemaField('order_in_segment', 'INT64')
    #     ]
    #     self.data['idx_TL'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" TL ", x)])
    #     count = 0
    #     for ncol in self.data.index:
    #         bp, dt, dt_str, mfile = self._parse_entry_details(ncol=ncol)
    #         tl_index, tl_records = self._parse_seg_index(ncol=ncol, seg_list = ['idx_TL'])
    #         if tl_records != 0:
    #             count_tl = 1
    #             for i in tl_index:
    #                 segment = mfile[i:]
    #                 self.tali.loc[count, 'bus_ptnr'] = bp
    #                 self.tali.loc[count, 'file_date'] = dt
    #                 self.tali.loc[count, 'foreign_bureau_code'] = segment[3:4]
    #                 self.tali.loc[count, 'date_filed'] = segment[8:12] + segment[5:7]
    #                 self.tali.loc[count, 'name_court'] = segment[13:33]
    #                 self.tali.loc[count, 'court_number'] = segment[46:56]
    #                 self.tali.loc[count, 'amount'] = segment[57:63]
    #                 self.tali.loc[count, 'industry_code'] = segment[64:66]
    #                 self.tali.loc[count, 'date_released'] = segment[70:74] + segment[67:69]
    #                 self.tali.loc[count, 'date_verified'] = segment[83:87] + segment[80:82]
    #                 self.tali.loc[count, 'narrative_code_1'] = segment[88:90]
    #                 self.tali.loc[count, 'narrative_code_2'] = segment[91:93]
    #                 self.tali.loc[count, 'case_number'] = segment[94:136]
    #                 self.tali.loc[count, 'segment_code'] = 'TL'
    #                 self.tali.loc[count, 'segment_description'] = 'tax lien'
    #                 self.tali.loc[count, 'order_in_segment'] = count_tl
    #                 count += 1
    #                 count_tl += 1
    #     self.column_taboo.append('idx_TL')
    #     self.tali['check1'] = self.tali.date_filed.apply(FilterAndConverter.filter_6digits_date)
    #     self.tali['check2'] = self.tali.date_verified.apply(FilterAndConverter.filter_6digits_date)
    #     self.tali['check3'] = self.tali.date_released.apply(FilterAndConverter.filter_6digits_date)
    #     self.tali['check4'] = self.tali.industry_code.apply(FilterAndConverter.filter_industry_code)
    #     self.tali['check5'] = self.tali.narrative_code_1.apply(FilterAndConverter.filter_industry_code)
    #     self.tali['check6'] = self.tali.narrative_code_2.apply(FilterAndConverter.filter_industry_code)
    #     self.tali = self.tali.loc[self.tali.check1 & self.tali.check2 & self.tali.check3 & self.tali.check4 & self.tali.check5 & self.tali.check6]
    #     self.tali.drop(columns = ['check1', 'check2', 'check3', 'check4', 'check5', 'check6'], inplace=True)
    #     self.tali['amount'] = self.tali.amount.apply(FilterAndConverter.convert_amount_from_str).astype('Int64')
    #     self._push_seg_table(table=self.tali, table_len=len(self.tali), seg_name='20_tax_lien', schema=tali_scheme)      
   
    # 14. parsing financial counselor (FC), discountinued
    # def _parse_financial_counselor(self):
    #     self.ficl = pd.DataFrame(
    #         columns=['match_flag', 'bus_ptnr', 'file_date', 'foreign_bureau_code', 'date_reported', 'member_number', 'amount', 'date_checked', 
    #                  'date_settled', 'narrative_code_1', 'narrative_code_2', 'status_code', 'segment_code', 'segment_description', 'order_in_segment']
    #     )
    #     self.data['idx_FC'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" FC ", x)])
    #     count = 0
    #     for ncol in self.data.index:
    #         bp, dt, dt_str, mfile = self._parse_entry_details(ncol=ncol)
    #         fc_index, fc_records = self._parse_seg_index(ncol=ncol, seg_list = ['idx_FC'])
    #         if fc_records != 0:
    #             flag = ''
    #             count_fc = 1
    #             for i in fc_index:
    #                 segment = mfile[i:]
    #                 fg = f'FC{bp}{dt_str}{str(count).zfill(10)}'
    #                 self.ficl.loc[count, 'match_flag'] = fg
    #                 self.ficl.loc[count, 'bus_ptnr'] = bp
    #                 self.ficl.loc[count, 'file_date'] = dt
    #                 self.ficl.loc[count, 'foreign_bureau_code'] = segment[3:4]
    #                 self.ficl.loc[count, 'date_reported'] = segment[5:12]
    #                 self.ficl.loc[count, 'member_number'] = segment[13:23]
    #                 self.ficl.loc[count, 'amount'] = segment[24:30]
    #                 self.ficl.loc[count, 'date_checked'] = segment[31:38]
    #                 self.ficl.loc[count, 'date_settled'] = segment[39:46]
    #                 self.ficl.loc[count, 'narrative_code_1'] = segment[47:49]
    #                 self.ficl.loc[count, 'narrative_code_2'] = segment[50:52]
    #                 self.ficl.loc[count, 'status_code'] = segment[53:54]
    #                 self.ficl.loc[count, 'segment_code'] = 'FC'
    #                 self.ficl.loc[count, 'segment_description'] = 'financial counselor'
    #                 self.ficl.loc[count, 'order_in_segment'] = count_fc
    #                 count += 1
    #                 count_fc += 1
    #                 flag += f'{fg}, '
    #             self.data.loc[ncol, 'financial_counselor_flag'] = flag[:-2]
    #     self.column_taboo.append('idx_FC')
    #     self.ficl['check1'] = self.ficl.date_reported.apply(FilterAndConverter.filter_6digits_date)
    #     self.ficl['check2'] = self.ficl.date_checked.apply(FilterAndConverter.filter_6digits_date)
    #     self.ficl['check3'] = self.ficl.date_settled.apply(FilterAndConverter.filter_6digits_date)
    #     self.ficl['check4'] = self.ficl.status_code.apply(lambda x: x in ['S', 'I', 'V'])
    #     self.ficl['check5'] = self.ficl.member_number.apply(FilterAndConverter.filter_member_number)
    #     self.ficl = self.ficl.loc[self.ficl.check1 & self.ficl.check2 & self.ficl.check3 & self.ficl.check4 & self.ficl.check5]
    #     self.ficl.drop(columns = ['check1', 'check2', 'check3', 'check4', 'check5'], inplace=True)
    #     self._push_seg_table(table=self.ficl, table_len=len(self.ficl), seg_name='21_financial_counselor')      
    
    # 15. parsing garnishment
    def _parse_garnishment(self):
        self.garn = pd.DataFrame(
            columns=['bus_ptnr', 'file_date', 'foreign_bureau_code', 'date_reported', 'name_court', 'court_number', 'amount', 'date_satisfied', 
                     'date_checked', 'narrative_code_1', 'narrative_code_2', 'case_number', 'plaintiff', 'plaintiff_continued', 'garnishee', 
                     'defendant', 'segment_code', 'segment_description', 'order_in_segment']
        )
        garn_scheme = [
            SchemaField('bus_ptnr', 'STRING'),
            SchemaField('file_date', 'DATE'),
            SchemaField('foreign_bureau_code', 'STRING'),
            SchemaField('date_reported', 'STRING'),
            SchemaField('name_court', 'STRING'),
            SchemaField('court_number', 'STRING'),
            SchemaField('amount', 'NUMERIC'),
            SchemaField('date_satisfied', 'STRING'),
            SchemaField('date_checked', 'STRING'),
            SchemaField('narrative_code_1', 'STRING'),
            SchemaField('narrative_code_2', 'STRING'),
            SchemaField('case_number', 'STRING'),
            SchemaField('plaintiff', 'STRING'),
            SchemaField('plaintiff_continued', 'STRING'),
            SchemaField('garnishee', 'STRING'),
            SchemaField('defendant', 'STRING'),
            SchemaField('segment_code', 'STRING'),
            SchemaField('segment_description', 'STRING'),
            SchemaField('order_in_segment', 'INT64')
        ]
        self.data['idx_GN'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" GN ", x)])
        count = 0
        for ncol in self.data.index:
            bp, dt, dt_str, mfile = self._parse_entry_details(ncol=ncol)
            gn_index, gn_records = self._parse_seg_index(ncol=ncol, seg_list = ['idx_GN'])
            if gn_records != 0:
                count_gn = 1
                for i in gn_index:
                    segment = mfile[i:]
                    self.garn.loc[count, 'bus_ptnr'] = bp
                    self.garn.loc[count, 'file_date'] = dt
                    self.garn.loc[count, 'foreign_bureau_code'] = segment[3:4]
                    self.garn.loc[count, 'date_reported'] = segment[8:12] + segment[5:7]
                    self.garn.loc[count, 'name_court'] = segment[13:33]
                    self.garn.loc[count, 'court_number'] = segment[46:56]
                    self.garn.loc[count, 'amount'] = segment[57:63]
                    self.garn.loc[count, 'date_satisfied'] = segment[67:71] + segment[64:66]
                    self.garn.loc[count, 'date_checked'] = segment[75:79] + segment[72:74]
                    self.garn.loc[count, 'narrative_code_1'] = segment[80:82]
                    self.garn.loc[count, 'narrative_code_2'] = segment[83:85]
                    self.garn.loc[count, 'case_number'] = segment[86:128]
                    self.garn.loc[count, 'plaintiff'] = segment[129:159]
                    self.garn.loc[count, 'plaintiff_continued'] = segment[160:172]
                    self.garn.loc[count, 'garnishee'] = segment[173:213]
                    self.garn.loc[count, 'defendant'] = segment[214:280]
                    self.garn.loc[count, 'segment_code'] = 'GN'
                    self.garn.loc[count, 'segment_description'] = 'garnishment'
                    self.garn.loc[count, 'order_in_segment'] = count_gn
                    count += 1
                    count_gn += 1
        self.column_taboo.append('idx_GN')
        self.garn['check1'] = self.garn.date_reported.apply(FilterAndConverter.filter_6digits_date)
        self.garn['check2'] = self.garn.date_checked.apply(FilterAndConverter.filter_6digits_date)
        self.garn['check3'] = self.garn.date_satisfied.apply(FilterAndConverter.filter_6digits_date)
        self.garn = self.garn.loc[self.garn.check1 & self.garn.check2 & self.garn.check3]
        self.garn.drop(columns = ['check1', 'check2', 'check3'], inplace=True)
        self.garn['amount'] = self.garn.amount.apply(FilterAndConverter.convert_amount_from_str).astype('Int64')
        self._push_seg_table(table=self.garn, table_len=len(self.garn), seg_name='22_garnishment', schema=garn_scheme)      

    # 16. parsing trade check
    def _parse_trade_check(self):
        self.tdck = pd.DataFrame(
            columns=['bus_ptnr', 'file_date', 'foreign_bureau_code', 'account_designator_code', 'autodata_indicator', 'name_member', 
                     'telephone_area_code', 'telephone_number', 'extension', 'member_number', 'date_reported', 'date_opened', 'high_credit', 
                     'terms', 'balance', 'past_due', 'type_code', 'rate_code', 'day_counter_30', 'day_counter_60', 'day_counter_90', 
                     'months_reviewed', 'date_last_activity', 'account_number', 'previous_high_rate_1', 'previous_high_date_1', 
                     'previous_high_rate_2', 'previous_high_date_2', 'previous_high_rate_3', 'previous_high_date_3', 'narrative_code_1', 
                     'narrative_code_2', 'segment_code', 'segment_description', 'order_in_segment']
        )
        tdck_scheme = [
            SchemaField('bus_ptnr', 'STRING'),
            SchemaField('file_date', 'DATE'),
            SchemaField('foreign_bureau_code', 'STRING'),
            SchemaField('account_designator_code', 'STRING'),
            SchemaField('autodata_indicator', 'STRING'),
            SchemaField('name_member', 'STRING'),
            SchemaField('telephone_area_code', 'STRING'),
            SchemaField('telephone_number', 'STRING'),
            SchemaField('extension', 'STRING'),
            SchemaField('member_number', 'STRING'),
            SchemaField('date_reported', 'STRING'),
            SchemaField('date_opened', 'STRING'),
            SchemaField('high_credit', 'NUMERIC'),
            SchemaField('terms', 'NUMERIC'),
            SchemaField('balance', 'NUMERIC'),
            SchemaField('past_due', 'NUMERIC'),
            SchemaField('type_code', 'STRING'),
            SchemaField('rate_code', 'STRING'),
            SchemaField('day_counter_30', 'INT64'),
            SchemaField('day_counter_60', 'INT64'),
            SchemaField('day_counter_90', 'INT64'),
            SchemaField('months_reviewed', 'INT64'),
            SchemaField('date_last_activity', 'STRING'),
            SchemaField('account_number', 'STRING'),
            SchemaField('previous_high_rate_1', 'FLOAT'),
            SchemaField('previous_high_date_1', 'STRING'),
            SchemaField('previous_high_rate_2', 'FLOAT'),
            SchemaField('previous_high_date_2', 'STRING'),
            SchemaField('previous_high_rate_3', 'FLOAT'),
            SchemaField('previous_high_date_3', 'STRING'),
            SchemaField('narrative_code_1', 'STRING'),
            SchemaField('narrative_code_2', 'STRING'),
            SchemaField('segment_code', 'STRING'),
            SchemaField('segment_description', 'STRING'),
            SchemaField('order_in_segment', 'INT64')
        ]
        self.data['idx_TC'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("TC ", x)])
        count = 0
        for ncol in self.data.index:
            bp, dt, dt_str, mfile = self._parse_entry_details(ncol=ncol)
            tc_index, tc_records = self._parse_seg_index(ncol=ncol, seg_list = ['idx_TC'])
            if tc_records != 0:
                count_tc = 1
                for i in tc_index:
                    segment = mfile[i:]
                    self.tdck.loc[count, 'bus_ptnr'] = bp
                    self.tdck.loc[count, 'file_date'] = dt
                    self.tdck.loc[count, 'foreign_bureau_code'] = segment[3:4]
                    self.tdck.loc[count, 'account_designator_code'] = segment[5:6]
                    self.tdck.loc[count, 'autodata_indicator'] = segment[6:7]
                    self.tdck.loc[count, 'name_member'] = segment[8:28]
                    self.tdck.loc[count, 'telephone_area_code'] = segment[29:32]
                    self.tdck.loc[count, 'telephone_number'] = segment[33:41]
                    self.tdck.loc[count, 'extension'] = segment[42:46]
                    self.tdck.loc[count, 'member_number'] = segment[47:57]
                    self.tdck.loc[count, 'date_reported'] = segment[61:65] + segment[58:60]
                    self.tdck.loc[count, 'date_opened'] = segment[69:73] + segment[66:68]
                    self.tdck.loc[count, 'high_credit'] = segment[74:79]
                    self.tdck.loc[count, 'terms'] = segment[80:84]
                    self.tdck.loc[count, 'balance'] = segment[85:90]
                    self.tdck.loc[count, 'past_due'] = segment[91:96]
                    self.tdck.loc[count, 'type_code'] = segment[97:98]
                    self.tdck.loc[count, 'rate_code'] = segment[98:99]
                    self.tdck.loc[count, 'day_counter_30'] = segment[100:102]
                    self.tdck.loc[count, 'day_counter_60'] = segment[103:105]
                    self.tdck.loc[count, 'day_counter_90'] = segment[106:108]
                    self.tdck.loc[count, 'months_reviewed'] = segment[109:111]
                    self.tdck.loc[count, 'date_last_activity'] = segment[115:119] + segment[112:114]
                    self.tdck.loc[count, 'account_number'] = segment[120:135]
                    self.tdck.loc[count, 'previous_high_rate_1'] = segment[161:162]
                    self.tdck.loc[count, 'previous_high_date_1'] = segment[166:170] + segment[163:165]
                    self.tdck.loc[count, 'previous_high_rate_2'] = segment[172:173]
                    self.tdck.loc[count, 'previous_high_date_2'] = segment[177:181] + segment[174:176]
                    self.tdck.loc[count, 'previous_high_rate_3'] = segment[183:184]
                    self.tdck.loc[count, 'previous_high_date_3'] = segment[188:192] + segment[185:187]
                    self.tdck.loc[count, 'narrative_code_1'] = segment[196:198]
                    self.tdck.loc[count, 'narrative_code_2'] = segment[199:201]
                    self.tdck.loc[count, 'segment_code'] = 'TC'
                    self.tdck.loc[count, 'segment_description'] = 'trade check'
                    self.tdck.loc[count, 'order_in_segment'] = count_tc
                    count += 1
                    count_tc += 1
        self.column_taboo.append('idx_TC')
        self.tdck['check1'] = self.tdck.autodata_indicator.apply(lambda x: x == '*')
        self.tdck['check2'] = self.tdck.account_designator_code.apply(lambda x: x in ['I', 'J', 'U'])
        self.tdck['check3'] = self.tdck.date_reported.apply(FilterAndConverter.filter_6digits_date)
        self.tdck['check4'] = self.tdck.date_opened.apply(FilterAndConverter.filter_6digits_date)
        self.tdck['check5'] = self.tdck.date_last_activity.apply(FilterAndConverter.filter_6digits_date)
        self.tdck['check6'] = self.tdck.previous_high_date_1.apply(FilterAndConverter.filter_6digits_date)
        self.tdck['check7'] = self.tdck.previous_high_date_2.apply(FilterAndConverter.filter_6digits_date)
        self.tdck['check8'] = self.tdck.previous_high_date_3.apply(FilterAndConverter.filter_6digits_date)
        self.tdck = self.tdck.loc[(self.tdck.check1 | self.tdck.check2) & self.tdck.check3 & self.tdck.check4 & self.tdck.check5 & self.tdck.check6 & self.tdck.check7 & self.tdck.check8]
        self.tdck.drop(columns = ['check3', 'check4', 'check5', 'check6', 'check7', 'check8'], inplace=True)
        self.tdck['day_counter_30'] = self.tdck.day_counter_30.apply(FilterAndConverter.convert_int_with_missing).astype('Int64')
        self.tdck['day_counter_60'] = self.tdck.day_counter_60.apply(FilterAndConverter.convert_int_with_missing).astype('Int64')
        self.tdck['day_counter_90'] = self.tdck.day_counter_90.apply(FilterAndConverter.convert_int_with_missing).astype('Int64')
        self.tdck['months_reviewed'] = self.tdck.months_reviewed.apply(FilterAndConverter.convert_int_with_missing).astype('Int64')
        self.tdck['previous_high_rate_1'] = self.tdck.previous_high_rate_1.apply(FilterAndConverter.convert_float_with_missing)
        self.tdck['previous_high_rate_2'] = self.tdck.previous_high_rate_2.apply(FilterAndConverter.convert_float_with_missing)
        self.tdck['previous_high_rate_3'] = self.tdck.previous_high_rate_3.apply(FilterAndConverter.convert_float_with_missing)
        self.tdck['high_credit'] = self.tdck.high_credit.apply(FilterAndConverter.convert_amount_from_str).astype('Int64')
        self.tdck['terms'] = self.tdck.terms.apply(FilterAndConverter.convert_amount_from_str).astype('Int64')
        self.tdck['balance'] = self.tdck.balance.apply(FilterAndConverter.convert_amount_from_str).astype('Int64')
        self.tdck['past_due'] = self.tdck.past_due.apply(FilterAndConverter.convert_amount_from_str).astype('Int64')
        self._push_seg_table(table=self.tdck, table_len=len(self.tdck), seg_name='23_trade_check_for_check', schema=tdck_scheme)      

    # 17. parsing nonmember trade check (NT), discountinued
    # def _parse_nonmember_trade_check(self):
    #     self.ntdck = pd.DataFrame(
    #         columns=['match_flag', 'bus_ptnr', 'file_date', 'date_reported', 'type_code', 'rating_code_0_or_greater', 
    #                  'rating_code_less_than_0', 'date_opened', 'narrative_code_1', 'narrative_code_2', 'customer_narrative', 
    #                  'high_credit_amount', 'balance', 'past_due_amount', 'segment_code', 'segment_description', 'order_in_segment']
    #     )
    #     self.data['idx_NT'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" NT ", x)])
    #     count = 0
    #     for ncol in self.data.index:
    #         bp, dt, dt_str, mfile = self._parse_entry_details(ncol=ncol)
    #         nt_index, nt_records = self._parse_seg_index(ncol=ncol, seg_list = ['idx_NT'])
    #         if nt_records != 0:
    #             flag = ''
    #             count_nt = 1
    #             for i in nt_index:
    #                 segment = mfile[i:]
    #                 self.ntdck.loc[count, 'bus_ptnr'] = bp
    #                 self.ntdck.loc[count, 'file_date'] = dt
    #                 self.ntdck.loc[count, 'date_reported'] = segment[3:10]
    #                 self.ntdck.loc[count, 'type_code'] = segment[11:12]
    #                 self.ntdck.loc[count, 'rating_code_0_or_greater'] = segment[13:14]
    #                 self.ntdck.loc[count, 'rating_code_less_than_0'] = segment[15:16]
    #                 self.ntdck.loc[count, 'date_opened'] = segment[17:24]
    #                 self.ntdck.loc[count, 'narrative_code_1'] = segment[25:27]
    #                 self.ntdck.loc[count, 'narrative_code_2'] = segment[28:30]
    #                 self.ntdck.loc[count, 'customer_narrative'] = segment[31:71]
    #                 self.ntdck.loc[count, 'high_credit_amount'] = segment[71:78]
    #                 self.ntdck.loc[count, 'balance'] = segment[80:86]
    #                 self.ntdck.loc[count, 'past_due_amount'] = segment[87:93]
    #                 self.ntdck.loc[count, 'segment_code'] = 'NT'
    #                 self.ntdck.loc[count, 'segment_description'] = 'non-member trade check'
    #                 self.ntdck.loc[count, 'order_in_segment'] = count_nt
    #                 count += 1
    #                 count_nt += 1
    #     self.column_taboo.append('idx_NT')
    #     self.ntdck['check1'] = self.ntdck.date_reported.apply(FilterAndConverter.filter_6digits_date)
    #     self.ntdck['check2'] = self.ntdck.date_opened.apply(FilterAndConverter.filter_6digits_date)
    #     self.ntdck = self.ntdck.loc[self.ntdck.check1 & self.ntdck.check2]
    #     self.ntdck.drop(columns = ['check1', 'check2'], inplace=True)
    #     self._push_seg_table(table=self.ntdck, table_len=len(self.ntdck), seg_name='24_nonmember_trade_check')      
    
    # 18. parsing chequing and saving
    def _parse_chequing_saving(self):
        self.chsv = pd.DataFrame(
            columns=['bus_ptnr', 'file_date', 'foreign_bureau_code', 'date_reported', 'name_member', 'telephone_area_code', 
                     'telephone_number', 'extension', 'member_number', 'date_opened', 'amount', 'type_account', 'narrative_code_1', 
                     'status_code', 'nsf_information', 'account_number', 'segment_code', 'segment_description', 'order_in_segment']
        )
        chsv_scheme = [
            SchemaField('bus_ptnr', 'STRING'),
            SchemaField('file_date', 'DATE'),
            SchemaField('foreign_bureau_code', 'STRING'),
            SchemaField('date_reported', 'STRING'),
            SchemaField('name_member', 'STRING'),
            SchemaField('telephone_area_code', 'STRING'),
            SchemaField('telephone_number', 'STRING'),
            SchemaField('extension', 'STRING'),
            SchemaField('member_number', 'STRING'),
            SchemaField('date_opened', 'STRING'),
            SchemaField('amount', 'STRING'),
            SchemaField('type_account', 'STRING'),
            SchemaField('narrative_code_1', 'STRING'),
            SchemaField('status_code', 'STRING'),
            SchemaField('nsf_information', 'STRING'),
            SchemaField('account_number', 'STRING'),
            SchemaField('segment_code', 'STRING'),
            SchemaField('segment_description', 'STRING'),
            SchemaField('order_in_segment', 'INT64')
        ]
        self.data['idx_CS'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" CS ", x)])
        count = 0
        for ncol in self.data.index:
            bp, dt, dt_str, mfile = self._parse_entry_details(ncol=ncol)
            cs_index, cs_records = self._parse_seg_index(ncol=ncol, seg_list = ['idx_CS'])
            if cs_records != 0:
                count_cs = 1
                for i in cs_index:
                    segment = mfile[i:]
                    self.chsv.loc[count, 'bus_ptnr'] = bp
                    self.chsv.loc[count, 'file_date'] = dt
                    self.chsv.loc[count, 'foreign_bureau_code'] = segment[3:4]
                    self.chsv.loc[count, 'date_reported'] = segment[8:12] + segment[5:7]
                    self.chsv.loc[count, 'name_member'] = segment[13:33]
                    self.chsv.loc[count, 'telephone_area_code'] = segment[34:37]
                    self.chsv.loc[count, 'telephone_number'] = segment[38:46]
                    self.chsv.loc[count, 'extension'] = segment[47:51]
                    self.chsv.loc[count, 'member_number'] = segment[52:62]
                    self.chsv.loc[count, 'date_opened'] = segment[66:70] + segment[63:65]
                    self.chsv.loc[count, 'amount'] = segment[80:95]
                    self.chsv.loc[count, 'type_account'] = segment[96:97]
                    self.chsv.loc[count, 'narrative_code_1'] = segment[98:100]
                    self.chsv.loc[count, 'status_code'] = segment[101:102]
                    self.chsv.loc[count, 'nsf_information'] = segment[103:118]
                    self.chsv.loc[count, 'account_number'] = segment[119:134]
                    self.chsv.loc[count, 'segment_code'] = 'CS'
                    self.chsv.loc[count, 'segment_description'] = 'chequing and saving'
                    self.chsv.loc[count, 'order_in_segment'] = count_cs
                    count += 1
                    count_cs += 1
        self.column_taboo.append('idx_CS')
        self.chsv['check1'] = self.chsv.date_reported.apply(FilterAndConverter.filter_6digits_date)
        self.chsv['check2'] = self.chsv.date_opened.apply(FilterAndConverter.filter_6digits_date)
        self.chsv['check3'] = self.chsv.member_number.apply(FilterAndConverter.filter_member_number)
        self.chsv['check4'] = self.chsv.type_account.apply(lambda x: x in 'ABCDEFGHIJKLMNOPQSTUVWXY ')
        self.chsv['check5'] = self.chsv.status_code.apply(lambda x: x in 'ABCDQTUXZ ')
        self.chsv['check6'] = self.chsv.narrative_code_1.apply(FilterAndConverter.filter_industry_code)
        self.chsv = self.chsv.loc[self.chsv.check1 & self.chsv.check2 & self.chsv.check3 & self.chsv.check4 & self.chsv.check5 & self.chsv.check6]
        self.chsv.drop(columns = ['check1', 'check2', 'check3', 'check4', 'check5', 'check6'], inplace=True)
        self._push_seg_table(table=self.chsv, table_len=len(self.chsv), seg_name='25_chequing_saving', schema=chsv_scheme)      

    # 19. parsing foreign bureau (FB), discountinued, but foreign bureau inquries is valid
    def _parse_foreign_bureau(self):
        # self.frbr = pd.DataFrame(
        #     columns=['match_flag', 'bus_ptnr', 'file_date', 'date_reported_or_inquries', 'foreign_bureau_code', 
        #              'city_narrative', 'province_narrative', 'segment_code', 'segment_description', 'order_in_segment']
        # )
        self.frbr = pd.DataFrame(
            columns=['bus_ptnr', 'file_date', 'date_inquiry', 'city_narrative', 'province_narrative', 
                     'segment_code', 'segment_description', 'order_in_segment']
        )
        frbr_scheme = [
            SchemaField('bus_ptnr', 'STRING'),
            SchemaField('file_date', 'DATE'),
            SchemaField('date_inquiry', 'DATE'),
            SchemaField('city_narrative', 'STRING'),
            SchemaField('province_narrative', 'STRING'),
            SchemaField('segment_code', 'STRING'),
            SchemaField('segment_description', 'STRING'),
            SchemaField('order_in_segment', 'INT64')
        ]
        # self.data['idx_FB'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" FB ", x)])
        self.data['idx_FI'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" FI ", x)])
        count = 0
        for ncol in self.data.index:
            bp, dt, dt_str, mfile = self._parse_entry_details(ncol=ncol)
            # fb_index, fb_records, fi_index, fi_records = self._parse_seg_index(ncol=ncol, seg_list = ['idx_FB', 'idx_FI'])
            fi_index, fi_records = self._parse_seg_index(ncol=ncol, seg_list = ['idx_FI'])
            # if fb_records + fi_records > 0:
            if fi_records != 0:
                # if fb_records != 0:
                    # count_fb = 1
                    # for i in fb_index:
                    #     segment = mfile[i:]
                    #     fg = f'FB{bp}{dt_str}{str(count).zfill(10)}'
                    #     self.frbr.loc[count, 'match_flag'] = fg
                    #     self.frbr.loc[count, 'bus_ptnr'] = bp
                    #     self.frbr.loc[count, 'file_date'] = dt
                    #     self.frbr.loc[count, 'date_reported_or_inquries'] = segment[3:10]
                    #     self.frbr.loc[count, 'foreign_bureau_code'] = segment[11:12]
                    #     self.frbr.loc[count, 'city_narrative'] = segment[13:31]
                    #     self.frbr.loc[count, 'province_narrative'] = segment[32:72]
                    #     self.frbr.loc[count, 'segment_code'] = 'FB'
                    #     self.frbr.loc[count, 'segment_description'] = 'foreign bureau'
                    #     self.frbr.loc[count, 'order_in_segment'] = count_fb
                    #     count += 1
                    #     count_fb += 1
                # if fi_records != 0:
                count_fi = 1
                for i in fi_index:
                    segment = mfile[i:]
                    self.frbr.loc[count, 'bus_ptnr'] = bp
                    self.frbr.loc[count, 'file_date'] = dt
                    self.frbr.loc[count, 'date_inquiry'] = segment[3:13]
                    self.frbr.loc[count, 'city_narrative'] = segment[14:32]
                    self.frbr.loc[count, 'province_narrative'] = segment[33:53]
                    self.frbr.loc[count, 'segment_code'] = 'FI'
                    self.frbr.loc[count, 'segment_description'] = 'foreign bureau inquries'
                    self.frbr.loc[count, 'order_in_segment'] = count_fi
                    count += 1
                    count_fi += 1
        # self.column_taboo += ['idx_FB', 'idx_FI']
        self.column_taboo.append('idx_FI')
        self.frbr['date_inquiry'] = self.frbr.date_inquiry.apply(FilterAndConverter.convert_8digits_date)
        self._push_seg_table(table=self.frbr, table_len=len(self.frbr), seg_name='27_foreign_bureau', schema=frbr_scheme)      
     
    # 20. parsing local special service
    def _parse_locate_special_service(self):
        self.lssv = pd.DataFrame(
            columns=['bus_ptnr', 'file_date', 'date_reported', 'name_member', 'telephone_area_code', 'telephone_number', 
                     'extension', 'member_number', 'type_code', 'segment_code', 'segment_description', 'order_in_segment']
        )
        lssv_scheme = [
            SchemaField('bus_ptnr', 'STRING'),
            SchemaField('file_date', 'DATE'),
            SchemaField('date_reported', 'STRING'),
            SchemaField('name_member', 'STRING'),
            SchemaField('telephone_area_code', 'STRING'),
            SchemaField('telephone_number', 'STRING'),
            SchemaField('extension', 'STRING'),
            SchemaField('member_number', 'STRING'),
            SchemaField('type_code', 'STRING'),
            SchemaField('segment_code', 'STRING'),
            SchemaField('segment_description', 'STRING'),
            SchemaField('order_in_segment', 'INT64')
        ]
        self.data['idx_LO'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" LO ", x)])
        count = 0
        for ncol in self.data.index:
            bp, dt, dt_str, mfile = self._parse_entry_details(ncol=ncol)
            lo_index, lo_records = self._parse_seg_index(ncol=ncol, seg_list = ['idx_LO'])
            if lo_records != 0:
                count_lo = 1
                for i in lo_index:
                    segment = mfile[i:]
                    self.lssv.loc[count, 'bus_ptnr'] = bp
                    self.lssv.loc[count, 'file_date'] = dt
                    self.lssv.loc[count, 'date_reported'] = segment[3:10]
                    self.lssv.loc[count, 'name_member'] = segment[11:31]
                    self.lssv.loc[count, 'telephone_area_code'] = segment[32:35]
                    self.lssv.loc[count, 'telephone_number'] = segment[36:44]
                    self.lssv.loc[count, 'extension'] = segment[45:49]
                    self.lssv.loc[count, 'member_number'] = segment[50:60]
                    self.lssv.loc[count, 'type_code'] = segment[61:62]
                    self.lssv.loc[count, 'segment_code'] = 'LO'
                    self.lssv.loc[count, 'segment_description'] = 'local or special service'
                    self.lssv.loc[count, 'order_in_segment'] = count_lo
                    count += 1
                    count_lo += 1
        self.column_taboo.append('idx_LO')
        self.lssv['check1'] = self.lssv.date_reported.apply(FilterAndConverter.filter_6digits_date)
        self.lssv['check2'] = self.lssv.member_number.apply(FilterAndConverter.filter_member_number)
        self.lssv = self.lssv.loc[self.lssv.check1 & self.lssv.check2]
        self.lssv.drop(columns = ['check1', 'check2'], inplace=True)
        self._push_seg_table(table=self.lssv, table_len=len(self.lssv), seg_name='28_locate_special_service', schema=lssv_scheme)      
    
    # 21. parsing inquries
    def _parse_inquries(self):
        self.inqr = pd.DataFrame(
            columns=['bus_ptnr', 'file_date', 'date_inquiry', 'name_member', 'telephone_area_code', 'telephone_number', 
                     'extension', 'member_number', 'segment_code', 'segment_description', 'order_in_segment']
        )
        inqr_scheme = [
            SchemaField('bus_ptnr', 'STRING'),
            SchemaField('file_date', 'DATE'),
            SchemaField('date_inquiry', 'DATE'),
            SchemaField('name_member', 'STRING'),
            SchemaField('telephone_area_code', 'STRING'),
            SchemaField('name_member', 'STRING'),
            SchemaField('telephone_area_code', 'STRING'),
            SchemaField('telephone_number', 'STRING'),
            SchemaField('extension', 'STRING'),
            SchemaField('member_number', 'STRING'),
            SchemaField('segment_code', 'STRING'),
            SchemaField('segment_description', 'STRING'),
            SchemaField('order_in_segment', 'INT64')
        ]
        self.data['idx_IQ'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" IQ ", x)])
        count = 0
        for ncol in self.data.index:
            bp, dt, dt_str, mfile = self._parse_entry_details(ncol=ncol)
            iq_index, iq_records = self._parse_seg_index(ncol=ncol, seg_list = ['idx_IQ'])
            if iq_records != 0:
                count_iq = 1
                for i in iq_index:
                    segment = mfile[i:]
                    self.inqr.loc[count, 'bus_ptnr'] = bp
                    self.inqr.loc[count, 'file_date'] = dt
                    self.inqr.loc[count, 'date_inquiry'] = segment[3:13]
                    self.inqr.loc[count, 'name_member'] = segment[14:34]
                    self.inqr.loc[count, 'telephone_area_code'] = segment[35:38]
                    self.inqr.loc[count, 'telephone_number'] = segment[39:47]
                    self.inqr.loc[count, 'extension'] = segment[48:52]
                    self.inqr.loc[count, 'member_number'] = segment[53:63]
                    self.inqr.loc[count, 'segment_code'] = 'IQ'
                    self.inqr.loc[count, 'segment_description'] = 'inquries'
                    self.inqr.loc[count, 'order_in_segment'] = count_iq
                    count += 1
                    count_iq += 1
        self.column_taboo.append('idx_IQ')
        self.inqr['check1'] = self.inqr.member_number.apply(FilterAndConverter.filter_member_number)
        self.inqr = self.inqr.loc[self.inqr.check1]
        self.inqr.drop(columns = 'check1', inplace=True)
        self.inqr['date_inquiry'] = self.inqr.date_inquiry.apply(FilterAndConverter.convert_8digits_date)
        self._push_seg_table(table=self.inqr, table_len=len(self.inqr), seg_name='29_inquries', schema=inqr_scheme)      
    
    # 22. parsing consumer declaration
    def _parse_consumer_declaration(self):
        self.csdc = pd.DataFrame(
            columns=['bus_ptnr', 'file_date', 'date_reported', 'date_purged', 'declaration', 'declaration_continued_1',
                     'declaration_continued_2', 'declaration_continued_3', 'declaration_continued_4', 'declaration_continued_end', 
                     'segment_code', 'segment_description', 'order_in_segment']
        )
        csdc_scheme = [
            SchemaField('bus_ptnr', 'STRING'),
            SchemaField('file_date', 'DATE'),
            SchemaField('date_reported', 'STRING'),
            SchemaField('date_purged', 'STRING'),
            SchemaField('declaration', 'STRING'),
            SchemaField('declaration_continued_1', 'STRING'),
            SchemaField('declaration_continued_2', 'STRING'),
            SchemaField('declaration_continued_3', 'STRING'),
            SchemaField('declaration_continued_4', 'STRING'),
            SchemaField('declaration_continued_end', 'STRING'),
            SchemaField('segment_code', 'STRING'),
            SchemaField('segment_description', 'STRING'),
            SchemaField('order_in_segment', 'INT64')
        ]
        self.data['idx_CD'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" CD ", x)])
        count = 0
        for ncol in self.data.index:
            bp, dt, dt_str, mfile = self._parse_entry_details(ncol=ncol)
            cd_index, cd_records = self._parse_seg_index(ncol=ncol, seg_list = ['idx_CD'])
            if cd_index != 0:
                flag = ''
                count_cd = 1
                for i in cd_index:
                    segment = mfile[i:]
                    self.csdc.loc[count, 'bus_ptnr'] = bp
                    self.csdc.loc[count, 'file_date'] = dt
                    self.csdc.loc[count, 'date_reported'] = segment[6:10] + segment[3:5]
                    self.csdc.loc[count, 'date_purged'] = segment[14:18] + segment[11:13]
                    self.csdc.loc[count, 'declaration'] = segment[19:79]
                    self.csdc.loc[count, 'declaration_continued_1'] = segment[80:158]
                    self.csdc.loc[count, 'declaration_continued_2'] = segment[160:238]
                    self.csdc.loc[count, 'declaration_continued_3'] = segment[240:318]
                    self.csdc.loc[count, 'declaration_continued_4'] = segment[320:398]
                    self.csdc.loc[count, 'declaration_continued_end'] = segment[400:428]
                    self.csdc.loc[count, 'segment_code'] = 'CD'
                    self.csdc.loc[count, 'segment_description'] = 'consumer declaration'
                    self.csdc.loc[count, 'order_in_segment'] = count_cd
                    count += 1
                    count_cd += 1
        self.column_taboo.append('idx_CD')
        self.csdc['check1'] = self.csdc.date_reported.apply(FilterAndConverter.filter_6digits_date)
        self.csdc['check2'] = self.csdc.date_purged.apply(FilterAndConverter.filter_6digits_date)
        self.csdc = self.csdc.loc[self.csdc.check1 & self.csdc.check2]
        self.csdc.drop(columns = ['check1', 'check2'], inplace=True)
        self._push_seg_table(table=self.csdc, table_len=len(self.csdc),  seg_name='30_consumer_declaration', schema=csdc_scheme)

    # 23. parsing bureau score
    def _parse_bureau_score(self):
        self.busc = pd.DataFrame(
            columns=['bus_ptnr', 'file_date', 'product_score', 'first_reason_code', 'second_reason_code', 
                     'third_reason_code', 'fourth_reason_code', 'reject_message_code', 'reserved', 'product_identifier', 
                     'segment_code', 'segment_description', 'order_in_segment']
        )
        busc_scheme = [
            SchemaField('bus_ptnr', 'STRING'),
            SchemaField('file_date', 'DATE'),
            SchemaField('product_score', 'INT64'),
            SchemaField('first_reason_code', 'STRING'),
            SchemaField('second_reason_code', 'STRING'),
            SchemaField('third_reason_code', 'STRING'),
            SchemaField('fourth_reason_code', 'STRING'),
            SchemaField('reject_message_code', 'STRING'),
            SchemaField('reserved', 'STRING'),
            SchemaField('product_identifier', 'STRING'),
            SchemaField('segment_code', 'STRING'),
            SchemaField('segment_description', 'STRING'),
            SchemaField('order_in_segment', 'INT64')
        ]
        self.data['idx_BS'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" BS ", x)])
        count = 0
        for ncol in self.data.index:
            bp, dt, dt_str, mfile = self._parse_entry_details(ncol=ncol)
            bs_index, bs_records = self._parse_seg_index(ncol=ncol, seg_list = ['idx_BS'])
            if bs_records != 0:
                count_bs = 1
                for i in bs_index:
                    segment = mfile[i:]
                    self.busc.loc[count, 'bus_ptnr'] = bp
                    self.busc.loc[count, 'file_date'] = dt
                    self.busc.loc[count, 'product_score'] = segment[3:8]
                    self.busc.loc[count, 'first_reason_code'] = segment[9:11]
                    self.busc.loc[count, 'second_reason_code'] = segment[12:14]
                    self.busc.loc[count, 'third_reason_code'] = segment[15:17]
                    self.busc.loc[count, 'fourth_reason_code'] = segment[18:20]
                    self.busc.loc[count, 'reject_message_code'] = segment[21:22]
                    self.busc.loc[count, 'reserved'] = segment[26:28]
                    self.busc.loc[count, 'product_identifier'] = segment[77:79]
                    self.busc.loc[count, 'segment_code'] = 'BS'
                    self.busc.loc[count, 'segment_description'] = 'bureau score'
                    self.busc.loc[count, 'order_in_segment'] = count_bs
                    count += 1
                    count_bs += 1
        self.column_taboo.append('idx_BS')
        self.busc['check1'] = self.busc.product_score.apply(lambda x: x.strip().isdigit() if x[0] not in ['+', '-'] else x.strip()[1:].isdigit())
        self.busc = self.busc.loc[self.busc.check1]
        self.busc.drop(columns = 'check1', inplace=True)
        self.busc['product_score'] = self.busc.product_score.apply(FilterAndConverter.convert_int_with_missing).astype('Int64')
        self._push_seg_table(table=self.busc, table_len=len(self.busc),  seg_name='31_bureau_score', schema=busc_scheme)     


# This is designed as in a monthly running frequency.
# Each time running this code, designate the year and the month of the data want to be retrieved
# Steps:
#     1. object instantiation with year and month
#     2. call push_tables_to_google_bigquery()
for year in [2021, 2022, 2023, 2024]:
    for month in range(1, 13):
        if year == 2024 and month >= 10:
            pass
        else:
            parser = FFFParser(begin_year=year, begin_month=month)
            parser.push_tables_to_google_bigquery()