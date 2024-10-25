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


# a parser function that helps parsing the mfile info
def parse_mfile(row, idx_li):
    li = []
    for idx in idx_li:
        li += row[idx]
    pk14 = row['mfile']
    return [pk14[idx:] for idx in li]

#  a mapping table
seg_abbr_dict = {
    'CA' : 'current address',
    'FA' : 'former address',
    'F2' : 'former address',
    'AK' : 'also known as',
    'FN' : 'also known as',
    'DT' : 'death',
    'ES' : 'current employment situation',
    'EF' : 'former employment situation',
    'E2' : 'former employment situation',
    'OI' : 'other income',
    'BP' : 'bankruptcy',
    'CO' : 'collection',
    'FM' : 'secured loan',
    'LI' : 'legal item',
    'MI' : 'marital item',
    'GN' : 'garnishment',
    'TC' : 'trade check',
    'CS' : 'chequing and saving',
    'FI' : 'foreign bureau inquries',
    'LO' : 'local or special service',
    'IQ' : 'inquries',
    'CD' : 'consumer declaration',
    'BS' : 'bureau score'
}

# A class that contains all common filters which used to guarantee the correctness of parsing
class Converters:
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
                 which_tables: list = None, push_header: bool = True, debug_mode: bool = False,
                 project_id: str = 'pd-deep-sat-10', dataset_id: str = 'cd_fang_z_sbx'):
        self.begin_year = begin_year
        self.begin_month = begin_month
        self.end_year = end_year
        self.end_month = end_month
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.push_header = push_header
        self.debug_mode = debug_mode
       
        self.fff_name = 'pd-deep-prd.equifax_eraw_SEC.equifax_retail_credit_scoring'
        self.bq_prefix = f'{self.project_id}.{self.dataset_id}'
        self.client = bigquery.Client(project=project_id)
       
        if which_tables is None:
            self.seg_names = ['address', 'name', 'death', 'employment', 'other_income', 'bankruptcy',
                              'collection', 'secured_loan', 'legal_item', 'marital_item', 'garnishment',
                              'trade_check', 'chequing_saving', 'foreign_bureau', 'inquries',
                              'locate_special_service', 'consumer_declaration', 'bureau_score']  
            # self.seg_names = ['address', 'name', 'death', 'employment', 'other_income', 'bankruptcy', 'collection',
            #                   'secured_loan', 'legal_item', 'foreclosure', 'non_responsibility', 'marital_item',
            #                   'tax_lien', 'financial_counselor', 'garnishment', 'trade_check', 'nonmember_trade_check',
            #                   'chequing_saving', 'foreign_bureau', 'inquries', 'locate_special_service',
            #                   'consumer_declaration', 'bureau_score']  
        else:
            self.seg_names = which_tables
           
        self.error_log_info = {
            'year': (self.begin_year, self.end_year),
            'month': (self.begin_month, self.end_month),
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
       
        self.header = self.raw_data[['id', 'file_name', 'file_date', 'business_partner_id', 'file_raw_content']].copy()
        self.header = self.header[~self.header['file_raw_content'].isna()]
        self.header['check'] = self.header.file_raw_content.apply(lambda x: 'FULL' in x)
        self.header = self.header.loc[self.header.check]
        self.header['mfile'] = None
        for col in self.header_cols_dict.keys():
            self.header[col] = None
           
        if self.end_year is not None and self.end_month is not None:
            print(f'******************** FFF data ({self.begin_year}.{self.begin_month} to {self.end_year}.{self.end_month}) has been retrieved ! ********************')
        else:
            print(f'******************** FFF data ({self.begin_year}.{self.begin_month}) has been retrieved ! ********************')
           
    def push_tables_to_google_bigquery(self, parse_header: bool = True):
        if parse_header:
            self._parse_header()
       
        # push segment tables        
        for seg in self.seg_names:
            exec(f'self._parse_{seg}()', {'self': self})
            self.error_log_info['already_pushed'].append(seg)
            self.error_log_info['left_pushed'].remove(seg)
            time.sleep(0.1)
           
        # push the header table
        if self.push_header:
            self.header.drop(columns=self.column_taboo, inplace=True)
            self.header.reset_index(drop=True)
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
            self.header['file_since_date'] = self.header['file_since_date'].apply(Converters.convert_8digits_date)
            self.header['last_activity_date'] = self.header['last_activity_date'].apply(Converters.convert_8digits_date)
            self.header['this_report_date'] = self.header['this_report_date'].apply(Converters.convert_8digits_date)
            self.header['subjects_birth_age_date'] = self.header['subjects_birth_age_date'].apply(Converters.convert_8digits_date)
            # if not self.debug_mode:
            #     push_job = self.client.load_table_from_dataframe(self.header, f'{self.bq_prefix}.fff_segment_0_header',
            #                                                      job_config=bigquery.LoadJobConfig(schema=header_scheme))  
            #     push_job.result()
            # else:
            #     time.sleep(1)
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
           
        if 'mfile' in list(self.header.columns):
            self.push_tables_to_google_bigquery(parse_header=False)
        else:
            self.push_tables_to_google_bigquery()      
               
    def _push_seg_table(self, table: pd.DataFrame, table_len: int, seg_name: str, schema: list):
        if table_len > 0:
            table.reset_index(drop=True)
            # if not self.debug_mode:
            #     push_job = self.client.load_table_from_dataframe(table, f'{self.bq_prefix}.fff_segment_{seg_name}',
            #                                                      job_config=bigquery.LoadJobConfig(schema=schema))
            #     push_job.result()
            # else:
            #     time.sleep(1)
            print(f'{seg_name} table has been pushed to BigQuery @ {self.bq_prefix}')
       
    def _construct_fetch_query(self) -> str:
        year1 = self.begin_year
        month1 = self.begin_month
        if self.end_year is not None and self.end_month is not None:
            year2 = self.end_year
            month2 = self.end_month
        else:
            year2 = self.begin_year
            month2 = self.begin_month            

        fetch_query = f"""
            SELECT * FROM `{self.fff_name}`
            WHERE file_date >= DATE(SAFE_CAST({year1} AS INT64), SAFE_CAST({month1} AS INT64), 1) AND
            file_date <= LAST_DAY(DATE(SAFE_CAST({year2} AS INT64), SAFE_CAST({month2} AS INT64), 1))
            ORDER BY file_date, business_partner_id
        """      
        if self.debug_mode:
            fetch_query += ' LIMIT 50'
           
        return fetch_query
   
    def _parse_entry_details(self, ncol: int) -> Tuple:
        bp = self.header.loc[ncol, 'id']
        dt = self.header.loc[ncol, 'file_date']
        dt_str = str(dt)[:4] + str(dt)[5:7] + str(dt)[8:]
        mfile = self.header.loc[ncol, 'mfile']
        return bp, dt, dt_str, mfile
   
    def _parse_seg_index(self, ncol: int, seg_list: list) -> list:
        output = []
        for i, seg in enumerate(seg_list):
            idx = self.header.loc[ncol, seg]
            output.append(idx)
            output.append(len(idx))
        return output
         
    # parsing header    
    def _parse_header(self):
        self.header['mfile'] = self.header['file_raw_content'].apply(lambda x: x[x.index('FULL'):])
        for key, val in self.header_cols_dict.items():
            self.header[key] = self.header.mfile.apply(lambda x: x[val[0]:val[1]])
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
        self.data = self.header.copy()
        self.data['idx_CA'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(' CA ', x)])    
        self.data['idx_FA'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(' FA ', x)])
        self.data['idx_F2'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(' F2 ', x)])
        self.data['nCA'] = self.data.idx_CA.apply(len)
        self.data['nFA'] = self.data.idx_FA.apply(len)
        self.data['nF2'] = self.data.idx_F2.apply(len)
        self.data['count'] = self.data['nCA']+ self.data['nFA'] + self.data['nF2']
        self.data['ms_list'] = self.data.apply(lambda x: parse_mfile(x, ['idx_CA','idx_FA','idx_F2']), axis=1)
        self.data['filter_nan'] = self.data.ms_list.apply(len)
        self.data = self.data.loc[self.data.filter_nan > 0]
       
        self.addr['ms'] = self.data['ms_list'].explode().reset_index(drop=True)
        self.addr['bus_ptnr'] = self.data['business_partner_id'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.addr['file_date'] = self.data['file_date'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.addr['street_number'] = self.addr.ms.apply(lambda x: x[3:13])
        self.addr['street_name_direction_apartment'] = self.addr.ms.apply(lambda x: x[14:40])
        self.addr['city'] = self.addr.ms.apply(lambda x: x[80:100])
        self.addr['province'] = self.addr.ms.apply(lambda x: x[101:103])
        self.addr['postal_code'] = self.addr.ms.apply(lambda x: x[104:110])
        self.addr['residence_since'] = self.addr.ms.apply(lambda x: x[114:118] + x[111:113])
        self.addr['indicator_code'] = self.addr.ms.apply(lambda x: x[118:119])
        self.addr['segment_code'] = self.addr.ms.apply(lambda x: x[:2])
        self.addr['segment_description'] = self.addr.segment_code.apply(lambda x: seg_abbr_dict[x])
        self.addr['order_in_segment'] = self.addr.groupby(['bus_ptnr', 'segment_code']).cumcount()+1
       
        self.addr.drop(columns=['ms'],inplace=True)
        self.data.drop(columns=['idx_CA', 'idx_FA', 'idx_F2', 'nCA', 'nFA', 'nF2', 'count', 'ms_list', 'filter_nan'], inplace=True)
        self._push_seg_table(table=self.addr, table_len=len(self.addr), seg_name='1_2_3_address', schema=addr_scheme)      
   
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
        self.data = self.header.copy()
        self.data['idx_AK'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" AK ", x)])
        self.data['idx_FN'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("FN ", x)])
        self.data['nAK'] = self.data.idx_AK.apply(len)
        self.data['nFN'] = self.data.idx_FN.apply(len)
        self.data['count'] = self.data['nAK']+ self.data['nFN']
        self.data['ms_list'] = self.data.apply(lambda x: parse_mfile(x, ['idx_AK','idx_FN']), axis=1)
        self.data['filter_nan'] = self.data.ms_list.apply(len)
        self.data = self.data.loc[self.data.filter_nan > 0]
       
        self.names['ms'] = self.data['ms_list'].explode().reset_index(drop=True)
        self.names['bus_ptnr'] = self.data['business_partner_id'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.names['file_date'] = self.data['file_date'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.names['last_name'] = self.names.ms.apply(lambda x: x[3:28])
        self.names['first_name'] = self.names.ms.apply(lambda x: x[29:44])
        self.names['middle_name_initial'] = self.names.ms.apply(lambda x: x[45:60])
        self.names['suffix'] = self.names.ms.apply(lambda x: x[61:63])
        self.names['spouse_name'] = self.names.ms.apply(lambda x: x[80:95])
        self.names['legal_name_change'] = self.names.ms.apply(lambda x: x[96:97])
        self.names['segment_code'] = self.names.ms.apply(lambda x: x[:2])
        self.names['segment_description'] = self.names.segment_code.apply(lambda x: seg_abbr_dict[x])
        self.names['order_in_segment'] = self.names.groupby(['bus_ptnr', 'segment_code']).cumcount()+1
       
        self.names.drop(columns=['ms'], inplace=True)
        self.data.drop(columns=['idx_AK', 'idx_FN', 'nAK', 'nFN', 'count', 'ms_list', 'filter_nan'], inplace=True)
        self._push_seg_table(table=self.names, table_len=len(self.names), seg_name='4_5_name', schema=name_scheme)      
             
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
        self.data = self.header.copy()
        self.data['idx_DT'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("DT ", x)])
        self.data['count'] = self.data.idx_DT.apply(len)
        self.data['ms_list'] = self.data.apply(lambda x: parse_mfile(x, ['idx_DT']), axis=1)
        self.data['filter_nan'] = self.data.ms_list.apply(len)
        self.data = self.data.loc[self.data.filter_nan > 0]
   
        self.death['ms'] = self.data['ms_list'].explode().reset_index(drop=True)
        self.death['bus_ptnr'] = self.data['business_partner_id'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.death['file_date'] = self.data['file_date'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.death['subject_death_date'] = self.death.ms.apply(lambda x: x[6:10] + x[3:5])
        self.death['segment_code'] = self.death.ms.apply(lambda x: x[:2])
        self.death['segment_description'] = self.death.segment_code.apply(lambda x: seg_abbr_dict[x])
        self.death['order_in_segment'] = self.death.groupby(['bus_ptnr', 'segment_code']).cumcount()+1
       
        self.death.drop(columns=['ms'],inplace=True)
        self.data.drop(columns=['idx_DT', 'count', 'ms_list', 'filter_nan'], inplace=True)
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
            SchemaField('monthly_salary', 'INT64'),
            SchemaField('monthly_salary_indicator', 'STRING'),
            SchemaField('date_left', 'STRING'),
            SchemaField('segment_code', 'STRING'),
            SchemaField('segment_description', 'STRING'),
            SchemaField('order_in_segment', 'INT64')
        ]
        self.data = self.header.copy()
        self.data['idx_ES'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("ES ", x)])
        self.data['idx_EF'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" EF ", x)])
        self.data['idx_E2'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" E2 ", x)])
        self.data['nES'] = self.data.idx_ES.apply(len)
        self.data['nEF'] = self.data.idx_EF.apply(len)
        self.data['nE2'] = self.data.idx_E2.apply(len)
        self.data['count'] = self.data['nES']+ self.data['nEF'] + self.data['nE2']        
        self.data['ms_list'] = self.data.apply(lambda x: parse_mfile(x, ['idx_ES', 'idx_EF', 'idx_E2']), axis=1)
        self.data['filter_nan'] = self.data.ms_list.apply(len)
        self.data = self.data.loc[self.data.filter_nan > 0]
   
        self.empl['ms'] = self.data['ms_list'].explode().reset_index(drop=True)
        self.empl['bus_ptnr'] = self.data['business_partner_id'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.empl['file_date'] = self.data['file_date'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.empl['occupation'] = self.empl.ms.apply(lambda x: x[3:37])
        self.empl['employer'] = self.empl.ms.apply(lambda x: x[38:72])
        self.empl['city_of_employment'] = self.empl.ms.apply(lambda x: x[80:88])
        self.empl['province_of_employment'] = self.empl.ms.apply(lambda x: x[89:91])
        self.empl['date_employed'] = self.empl.ms.apply(lambda x: x[95:99] + x[92:94])
        self.empl['date_verified'] = self.empl.ms.apply(lambda x: x[103:107] + x[100:102])
        self.empl['verification_status'] = self.empl.ms.apply(lambda x: x[108:109])
        self.empl['monthly_salary'] = self.empl.ms.apply(lambda x: x[110:118])
        self.empl['date_left'] = self.empl.ms.apply(lambda x: x[122:126] + x[119:121])
        self.empl['segment_code'] = self.empl.ms.apply(lambda x: x[:2])
        self.empl['segment_description'] = self.empl.segment_code.apply(lambda x: seg_abbr_dict[x])
        self.empl['order_in_segment'] = self.empl.groupby(['bus_ptnr', 'segment_code']).cumcount()+1
       
        self.empl.drop(columns=['ms'], inplace=True)
        self.empl['monthly_salary_indicator'] = self.empl.monthly_salary.apply(lambda x: 'NV' if x[-2:] == 'NV' else '')
        self.empl['monthly_salary'] = self.empl.monthly_salary.apply(lambda x: x[:-2] if x[-2:] == 'NV' else x)
        self.empl['monthly_salary'] = self.empl.monthly_salary.apply(Converters.convert_amount_from_str).astype('Int64')
        self.data.drop(columns=['idx_ES', 'idx_EF', 'idx_E2', 'nES', 'nEF', 'nE2', 'count', 'ms_list', 'filter_nan'], inplace=True)
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
            SchemaField('income_amount', 'INT64'),
            SchemaField('income_source', 'STRING'),
            SchemaField('date_verified', 'STRING'),
            SchemaField('verification_status', 'STRING'),
            SchemaField('segment_code', 'STRING'),
            SchemaField('segment_description', 'STRING'),
            SchemaField('order_in_segment', 'INT64')
        ]
        self.data = self.header.copy()
        self.data['idx_OI'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" OI ", x)])
        self.data['count'] = self.data.idx_OI.apply(len)
        self.data['ms_list'] = self.data.apply(lambda x: parse_mfile(x, ['idx_OI']), axis=1)
        self.data['filter_nan'] = self.data.ms_list.apply(len)
        self.data = self.data.loc[self.data.filter_nan > 0]
   
        self.oinc['ms'] = self.data['ms_list'].explode().reset_index(drop=True)
        self.oinc['bus_ptnr'] = self.data['business_partner_id'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.oinc['file_date'] = self.data['file_date'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.oinc['date_reported'] = self.oinc.ms.apply(lambda x: x[6:10] + x[3:5])
        self.oinc['income_amount'] = self.oinc.ms.apply(lambda x: x[11:17])
        self.oinc['income_source'] = self.oinc.ms.apply(lambda x: x[18:58])
        self.oinc['date_verified'] = self.oinc.ms.apply(lambda x: x[62:66] + x[59:61])
        self.oinc['verification_status'] = self.oinc.ms.apply(lambda x: x[67:68])
        self.oinc['segment_code'] = self.oinc.ms.apply(lambda x: x[:2])
        self.oinc['segment_description'] = self.oinc.segment_code.apply(lambda x: seg_abbr_dict[x])
        self.oinc['order_in_segment'] = self.oinc.groupby(['bus_ptnr', 'segment_code']).cumcount()+1
       
        self.oinc.drop(columns=['ms'], inplace=True)
        self.oinc['income_amount'] = self.oinc.income_amount.apply(Converters.convert_amount_from_str).astype('Int64')
        self.data.drop(columns=['idx_OI', 'count', 'ms_list', 'filter_nan'], inplace=True)
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
            SchemaField('amount_liability', 'INT64'),
            SchemaField('asset_amount', 'INT64'),
            SchemaField('date_settled', 'STRING'),
            SchemaField('narrative_code_1', 'STRING'),
            SchemaField('narrative_code_2', 'STRING'),
            SchemaField('case_number', 'STRING'),
            SchemaField('segment_code', 'STRING'),
            SchemaField('segment_description', 'STRING'),
            SchemaField('order_in_segment', 'INT64')
        ]
        self.data = self.header.copy()
        self.data['idx_BP'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("BP ", x)])
        self.data['count'] = self.data.idx_BP.apply(len)
        self.data['ms_list'] = self.data.apply(lambda x: parse_mfile(x, ['idx_BP']), axis=1)
        self.data['filter_nan'] = self.data.ms_list.apply(len)
        self.data = self.data.loc[self.data.filter_nan > 0]
   
        self.bkpt['ms'] = self.data['ms_list'].explode().reset_index(drop=True)
        self.bkpt['bus_ptnr'] = self.data['business_partner_id'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.bkpt['file_date'] = self.data['file_date'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.bkpt['foreign_bureau_code'] = self.bkpt.ms.apply(lambda x: x[3:4])
        self.bkpt['date_filed'] = self.bkpt.ms.apply(lambda x: x[8:12] + x[5:7])
        self.bkpt['name_court'] = self.bkpt.ms.apply(lambda x: x[13:33])
        self.bkpt['court_number'] = self.bkpt.ms.apply(lambda x: x[52:62])
        self.bkpt['type_bankruptcy'] = self.bkpt.ms.apply(lambda x: x[63:64])
        self.bkpt['deposition_codes'] = self.bkpt.ms.apply(lambda x: x[67:68])
        self.bkpt['amount_liability'] = self.bkpt.ms.apply(lambda x: x[69:75])
        self.bkpt['asset_amount'] = self.bkpt.ms.apply(lambda x: x[80:86])
        self.bkpt['date_settled'] = self.bkpt.ms.apply(lambda x: x[90:94] + x[87:89])
        self.bkpt['narrative_code_1'] = self.bkpt.ms.apply(lambda x: x[95:97])
        self.bkpt['narrative_code_2'] = self.bkpt.ms.apply(lambda x: x[101:143])
        self.bkpt['case_number'] = self.bkpt.ms.apply(lambda x: x[101:143])
        self.bkpt['segment_code'] = self.bkpt.ms.apply(lambda x: x[:2])
        self.bkpt['segment_description'] = self.bkpt.segment_code.apply(lambda x: seg_abbr_dict[x])
        self.bkpt['order_in_segment'] = self.bkpt.groupby(['bus_ptnr', 'segment_code']).cumcount()+1
       
        self.bkpt.drop(columns=['ms'], inplace=True)
        self.bkpt['amount_liability'] = self.bkpt.amount_liability.apply(Converters.convert_amount_from_str).astype('Int64')
        self.bkpt['asset_amount'] = self.bkpt.asset_amount.apply(Converters.convert_amount_from_str).astype('Int64')        
        self.data.drop(columns=['idx_BP', 'count', 'ms_list', 'filter_nan'], inplace=True)      
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
            SchemaField('amount', 'INT64'),
            SchemaField('balance', 'INT64'),
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
        self.data = self.header.copy()
        self.data['idx_CO'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" CO ", x)])
        self.data['count'] = self.data.idx_CO.apply(len)
        self.data['ms_list'] = self.data.apply(lambda x: parse_mfile(x, ['idx_CO']), axis=1)
        self.data['filter_nan'] = self.data.ms_list.apply(len)
        self.data = self.data.loc[self.data.filter_nan > 0]
   
        self.colt['ms'] = self.data['ms_list'].explode().reset_index(drop=True)
        self.colt['bus_ptnr'] = self.data['business_partner_id'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.colt['file_date'] = self.data['file_date'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.colt['foreign_bureau_code'] = self.colt.ms.apply(lambda x: x[3:4])
        self.colt['date_reported'] = self.colt.ms.apply(lambda x: x[8:12] + x[5:7])
        self.colt['name_member'] = self.colt.ms.apply(lambda x: x[13:33])
        self.colt['member_number'] = self.colt.ms.apply(lambda x: x[52:62])
        self.colt['amount'] = self.colt.ms.apply(lambda x: x[63:69])
        self.colt['balance'] = self.colt.ms.apply(lambda x: x[70:76])
        self.colt['type'] = self.colt.ms.apply(lambda x: x[77:78])
        self.colt['narrative_code_1'] = self.colt.ms.apply(lambda x: x[80:82])
        self.colt['narrative_code_2'] = self.colt.ms.apply(lambda x: x[83:85])
        self.colt['industry_code'] = self.colt.ms.apply(lambda x: x[86:88])
        self.colt['date_paid'] = self.colt.ms.apply(lambda x: x[94:98] + x[91:93])
        self.colt['date_last_payment'] = self.colt.ms.apply(lambda x: x[102:106] + x[99:101])
        self.colt['creditors_account_number_and_name'] = self.colt.ms.apply(lambda x: x[107:157])
        self.colt['ledger_number'] = self.colt.ms.apply(lambda x: x[160:177])
        self.colt['segment_code'] = self.colt.ms.apply(lambda x: x[:2])
        self.colt['segment_description'] = self.colt.segment_code.apply(lambda x: seg_abbr_dict[x])
        self.colt['order_in_segment'] = self.colt.groupby(['bus_ptnr', 'segment_code']).cumcount()+1
       
        self.colt.drop(columns=['ms'], inplace=True)
        self.colt['amount'] = self.colt.amount.apply(Converters.convert_amount_from_str).astype('Int64')
        self.colt['balance'] = self.colt.balance.apply(Converters.convert_amount_from_str).astype('Int64')        
        self.data.drop(columns=['idx_CO', 'count', 'ms_list', 'filter_nan'], inplace=True)      
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
        self.data = self.header.copy()
        self.data['idx_FM'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("FM ", x)])
        self.data['count'] = self.data.idx_FM.apply(len)
        self.data['ms_list'] = self.data.apply(lambda x: parse_mfile(x, ['idx_FM']), axis=1)
        self.data['filter_nan'] = self.data.ms_list.apply(len)
        self.data = self.data.loc[self.data.filter_nan > 0]
   
        self.selo['ms'] = self.data['ms_list'].explode().reset_index(drop=True)
        self.selo['bus_ptnr'] = self.data['business_partner_id'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.selo['file_date'] = self.data['file_date'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.selo['foreign_bureau_code'] = self.selo.ms.apply(lambda x: x[3:4])
        self.selo['date_filed'] = self.selo.ms.apply(lambda x: x[8:12] + x[5:7])
        self.selo['name_court'] = self.selo.ms.apply(lambda x: x[13:33])
        self.selo['court_number'] = self.selo.ms.apply(lambda x: x[52:62])
        self.selo['industry_code'] = self.selo.ms.apply(lambda x: x[63:65])
        self.selo['maturity_date'] = self.selo.ms.apply(lambda x: x[69:73] + x[66:68])
        self.selo['narrative_code_1'] = self.selo.ms.apply(lambda x: x[73:76])
        self.selo['narrative_code_2'] = self.selo.ms.apply(lambda x: x[77:79])
        self.selo['creditors_name_address_amount'] = self.selo.ms.apply(lambda x: x[80:140].lstrip())
        self.selo['segment_code'] = self.selo.ms.apply(lambda x: x[:2])
        self.selo['segment_description'] = self.selo.segment_code.apply(lambda x: seg_abbr_dict[x])
        self.selo['order_in_segment'] = self.selo.groupby(['bus_ptnr', 'segment_code']).cumcount()+1
       
        self.selo.drop(columns=['ms'], inplace=True)      
        self.data.drop(columns=['idx_FM', 'count', 'ms_list', 'filter_nan'], inplace=True)      
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
        self.data = self.header.copy()
        self.data['idx_LI'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("LI ", x)])
        self.data['count'] = self.data.idx_LI.apply(len)
        self.data['ms_list'] = self.data.apply(lambda x: parse_mfile(x, ['idx_LI']), axis=1)
        self.data['filter_nan'] = self.data.ms_list.apply(len)
        self.data = self.data.loc[self.data.filter_nan > 0]
   
        self.leit['ms'] = self.data['ms_list'].explode().reset_index(drop=True)
        self.leit['bus_ptnr'] = self.data['business_partner_id'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.leit['file_date'] = self.data['file_date'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.leit['foreign_bureau_code'] = self.leit.ms.apply(lambda x: x[3:4])
        self.leit['date_filed'] = self.leit.ms.apply(lambda x: x[8:12] + x[5:7])
        self.leit['name_court'] = self.leit.ms.apply(lambda x: x[13:33])
        self.leit['court_number'] = self.leit.ms.apply(lambda x: x[52:62])
        self.leit['amount'] = self.leit.ms.apply(lambda x: x[63:69])
        self.leit['type_code'] = self.leit.ms.apply(lambda x: x[70:71])
        self.leit['date_satisfied'] = self.leit.ms.apply(lambda x: x[75:79] + x[72:74])
        self.leit['status_code'] = self.leit.ms.apply(lambda x: x[80:81])
        self.leit['date_verified'] = self.leit.ms.apply(lambda x: x[85:89] + x[82:84])
        self.leit['narrative_code_1'] = self.leit.ms.apply(lambda x: x[90:92])
        self.leit['narrative_code_2'] = self.leit.ms.apply(lambda x: x[93:95])
        self.leit['defendant'] = self.leit.ms.apply(lambda x: x[96:136])
        self.leit['case_number'] = self.leit.ms.apply(lambda x: x[137:159])
        self.leit['case_number_continued'] = self.leit.ms.apply(lambda x: x[160:180])
        self.leit['plaintiff'] = self.leit.ms.apply(lambda x: x[181:221])
        self.leit['laywer_name_address'] = self.leit.ms.apply(lambda x: x[240:300])
        self.leit['segment_code'] = self.leit.ms.apply(lambda x: x[:2])
        self.leit['segment_description'] = self.leit.segment_code.apply(lambda x: seg_abbr_dict[x])
        self.leit['order_in_segment'] = self.leit.groupby(['bus_ptnr', 'segment_code']).cumcount()+1
       
        self.leit.drop(columns=['ms'], inplace=True)      
        self.leit['amount'] = self.leit.amount.apply(Converters.convert_amount_from_str).astype('Int64')
        self.data.drop(columns=['idx_LI', 'count', 'ms_list', 'filter_nan'], inplace=True)      
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
        self.data = self.header.copy()
        self.data['idx_MI'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" MI ", x)])
        self.data['count'] = self.data.idx_MI.apply(len)
        self.data['ms_list'] = self.data.apply(lambda x: parse_mfile(x, ['idx_MI']), axis=1)
        self.data['filter_nan'] = self.data.ms_list.apply(len)
        self.data = self.data.loc[self.data.filter_nan > 0]
   
        self.mari['ms'] = self.data['ms_list'].explode().reset_index(drop=True)
        self.mari['bus_ptnr'] = self.data['business_partner_id'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.mari['file_date'] = self.data['file_date'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.mari['foreign_bureau_code'] = self.mari.ms.apply(lambda x: x[3:4])
        self.mari['date_reported'] = self.mari.ms.apply(lambda x: x[8:12] + x[5:7])
        self.mari['name_court'] = self.mari.ms.apply(lambda x: x[13:33])
        self.mari['court_number'] = self.mari.ms.apply(lambda x: x[52:62])
        self.mari['telephone_area_code'] = self.mari.ms.apply(lambda x: x[34:37])
        self.mari['telephone_number'] = self.mari.ms.apply(lambda x: x[38:46])
        self.mari['extension'] = self.mari.ms.apply(lambda x: x[47:51])
        self.mari['member_number'] = self.mari.ms.apply(lambda x: x[52:62])
        self.mari['action_code'] = self.mari.ms.apply(lambda x: x[63:64])
        self.mari['date_verified'] = self.mari.ms.apply(lambda x: x[68:72] + x[65:67])
        self.mari['amount'] = self.mari.ms.apply(lambda x: x[80:122])
        self.mari['additional_details'] = self.mari.ms.apply(lambda x: x[160:200])
        self.mari['segment_code'] = self.mari.ms.apply(lambda x: x[:2])
        self.mari['segment_description'] = self.mari.segment_code.apply(lambda x: seg_abbr_dict[x])
        self.mari['order_in_segment'] = self.mari.groupby(['bus_ptnr', 'segment_code']).cumcount()+1
       
        self.mari.drop(columns=['ms'], inplace=True)      
        self.data.drop(columns=['idx_MI', 'count', 'ms_list', 'filter_nan'], inplace=True)      
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
    #         SchemaField('amount', 'INT64'),
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
    #     self.tali['amount'] = self.tali.amount.apply(Converters.convert_amount_from_str).astype('Int64')
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
    #             count_fc = 1
    #             for i in fc_index:
    #                 segment = mfile[i:]
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
    #     self.column_taboo.append('idx_FC')
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
            SchemaField('amount', 'INT64'),
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
        self.data = self.header.copy()
        self.data['idx_GN'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" GN ", x)])
        self.data['count'] = self.data.idx_GN.apply(len)
        self.data['ms_list'] = self.data.apply(lambda x: parse_mfile(x, ['idx_GN']), axis=1)
        self.data['filter_nan'] = self.data.ms_list.apply(len)
        self.data = self.data.loc[self.data.filter_nan > 0]
   
        self.garn['ms'] = self.data['ms_list'].explode().reset_index(drop=True)
        self.garn['bus_ptnr'] = self.data['business_partner_id'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.garn['file_date'] = self.data['file_date'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.garn['foreign_bureau_code'] = self.garn.ms.apply(lambda x: x[3:4])
        self.garn['date_reported'] = self.garn.ms.apply(lambda x: x[8:12] + x[5:7])
        self.garn['name_court'] = self.garn.ms.apply(lambda x: x[13:33])
        self.garn['court_number'] = self.garn.ms.apply(lambda x: x[46:56])
        self.garn['amount'] = self.garn.ms.apply(lambda x: x[57:63])
        self.garn['date_satisfied'] = self.garn.ms.apply(lambda x: x[67:71] + x[64:66])
        self.garn['date_checked'] = self.garn.ms.apply(lambda x: x[75:79] + x[72:74])
        self.garn['narrative_code_1'] = self.garn.ms.apply(lambda x: x[80:82])
        self.garn['narrative_code_2'] = self.garn.ms.apply(lambda x: x[83:85])
        self.garn['case_number'] = self.garn.ms.apply(lambda x: x[86:128])
        self.garn['plaintiff'] = self.garn.ms.apply(lambda x: x[129:159])
        self.garn['plaintiff_continued'] = self.garn.ms.apply(lambda x: x[160:172])
        self.garn['garnishee'] = self.garn.ms.apply(lambda x: x[173:213])
        self.garn['defendant'] = self.garn.ms.apply(lambda x: x[214:280])
        self.garn['segment_code'] = self.garn.ms.apply(lambda x: x[:2])
        self.garn['segment_description'] = self.garn.segment_code.apply(lambda x: seg_abbr_dict[x])
        self.garn['order_in_segment'] = self.garn.groupby(['bus_ptnr', 'segment_code']).cumcount()+1
       
        self.garn.drop(columns=['ms'], inplace=True)      
        self.data.drop(columns=['idx_GN', 'count', 'ms_list', 'filter_nan'], inplace=True)    
        self.garn['amount'] = self.garn.amount.apply(Converters.convert_amount_from_str).astype('Int64')
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
            SchemaField('high_credit', 'INT64'),
            SchemaField('terms', 'INT64'),
            SchemaField('balance', 'INT64'),
            SchemaField('past_due', 'INT64'),
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
        self.data = self.header.copy()
        self.data['idx_TC'] = self.data.mfile.apply(lambda x: [m.start() for m in re.finditer("TC ", x)])
        self.data['count'] = self.data.idx_TC.apply(len)
        self.data['ms_list'] = self.data.apply(lambda x: parse_mfile(x, ['idx_TC']), axis=1)
        self.data['filter_nan'] = self.data.ms_list.apply(len)
        self.data = self.data.loc[self.data.filter_nan > 0]
   
        self.tdck['ms'] = self.data['ms_list'].explode().reset_index(drop=True)
        self.tdck['bus_ptnr'] = self.data['business_partner_id'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.tdck['file_date'] = self.data['file_date'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.tdck['foreign_bureau_code'] = self.tdck.ms.apply(lambda x: x[3:4])
        self.tdck['account_designator_code'] = self.tdck.ms.apply(lambda x: x[5:6])
        self.tdck['autodata_indicator'] = self.tdck.ms.apply(lambda x: x[6:7])
        self.tdck['name_member'] = self.tdck.ms.apply(lambda x: x[8:28])
        self.tdck['telephone_area_code'] = self.tdck.ms.apply(lambda x: x[29:32])
        self.tdck['telephone_number'] = self.tdck.ms.apply(lambda x: x[33:41])
        self.tdck['extension'] = self.tdck.ms.apply(lambda x: x[42:46])
        self.tdck['member_number'] = self.tdck.ms.apply(lambda x: x[47:57])
        self.tdck['date_reported'] = self.tdck.ms.apply(lambda x: x[61:65] + x[58:60])
        self.tdck['date_opened'] = self.tdck.ms.apply(lambda x: x[69:73] + x[66:68])
        self.tdck['high_credit'] = self.tdck.ms.apply(lambda x: x[74:79])
        self.tdck['terms'] = self.tdck.ms.apply(lambda x: x[80:84])
        self.tdck['balance'] = self.tdck.ms.apply(lambda x: x[85:90])
        self.tdck['past_due'] = self.tdck.ms.apply(lambda x: x[91:96])
        self.tdck['type_code'] = self.tdck.ms.apply(lambda x: x[97:98])
        self.tdck['rate_code'] = self.tdck.ms.apply(lambda x: x[98:99])
        self.tdck['day_counter_30'] = self.tdck.ms.apply(lambda x: x[100:102])
        self.tdck['day_counter_60'] = self.tdck.ms.apply(lambda x: x[103:105])
        self.tdck['day_counter_90'] = self.tdck.ms.apply(lambda x: x[106:108])
        self.tdck['months_reviewed'] = self.tdck.ms.apply(lambda x: x[109:111])
        self.tdck['date_last_activity'] = self.tdck.ms.apply(lambda x: x[115:119] + x[112:114])
        self.tdck['account_number'] = self.tdck.ms.apply(lambda x: x[120:135])
        self.tdck['previous_high_rate_1'] = self.tdck.ms.apply(lambda x: x[161:162])
        self.tdck['previous_high_date_1'] = self.tdck.ms.apply(lambda x: x[166:170] + x[163:165])
        self.tdck['previous_high_rate_2'] = self.tdck.ms.apply(lambda x: x[172:173])
        self.tdck['previous_high_date_2'] = self.tdck.ms.apply(lambda x: x[177:181] + x[174:176])
        self.tdck['previous_high_rate_3'] = self.tdck.ms.apply(lambda x: x[183:184])
        self.tdck['previous_high_date_3'] = self.tdck.ms.apply(lambda x: x[188:192] + x[185:187])
        self.tdck['narrative_code_1'] = self.tdck.ms.apply(lambda x: x[196:198])
        self.tdck['narrative_code_2'] = self.tdck.ms.apply(lambda x: x[199:201])
        self.tdck['segment_code'] = self.tdck.ms.apply(lambda x: x[:2])
        self.tdck['segment_description'] = self.tdck.segment_code.apply(lambda x: seg_abbr_dict[x])
        self.tdck['order_in_segment'] = self.tdck.groupby(['bus_ptnr', 'segment_code']).cumcount()+1
       
        self.tdck.drop(columns=['ms'], inplace=True)      
        self.tdck['day_counter_30'] = self.tdck.day_counter_30.apply(Converters.convert_int_with_missing).astype('Int64')
        self.tdck['day_counter_60'] = self.tdck.day_counter_60.apply(Converters.convert_int_with_missing).astype('Int64')
        self.tdck['day_counter_90'] = self.tdck.day_counter_90.apply(Converters.convert_int_with_missing).astype('Int64')
        self.tdck['months_reviewed'] = self.tdck.months_reviewed.apply(Converters.convert_int_with_missing).astype('Int64')
        self.tdck['previous_high_rate_1'] = self.tdck.previous_high_rate_1.apply(Converters.convert_float_with_missing)
        self.tdck['previous_high_rate_2'] = self.tdck.previous_high_rate_2.apply(Converters.convert_float_with_missing)
        self.tdck['previous_high_rate_3'] = self.tdck.previous_high_rate_3.apply(Converters.convert_float_with_missing)
        self.tdck['high_credit'] = self.tdck.high_credit.apply(Converters.convert_amount_from_str).astype('Int64')
        self.tdck['terms'] = self.tdck.terms.apply(Converters.convert_amount_from_str).astype('Int64')
        self.tdck['balance'] = self.tdck.balance.apply(Converters.convert_amount_from_str).astype('Int64')
        self.tdck['past_due'] = self.tdck.past_due.apply(Converters.convert_amount_from_str).astype('Int64')
        self.data.drop(columns=['idx_TC', 'count', 'ms_list', 'filter_nan'], inplace=True)    
        self._push_seg_table(table=self.tdck, table_len=len(self.tdck), seg_name='23_trade_check', schema=tdck_scheme)      

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
        self.data = self.header.copy()
        self.data['idx_CS'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" CS ", x)])
        self.data['count'] = self.data.idx_CS.apply(len)
        self.data['ms_list'] = self.data.apply(lambda x: parse_mfile(x, ['idx_CS']), axis=1)
        self.data['filter_nan'] = self.data.ms_list.apply(len)
        self.data = self.data.loc[self.data.filter_nan > 0]
   
        self.chsv['ms'] = self.data['ms_list'].explode().reset_index(drop=True)
        self.chsv['bus_ptnr'] = self.data['business_partner_id'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.chsv['file_date'] = self.data['file_date'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.chsv['foreign_bureau_code'] = self.chsv.ms.apply(lambda x: x[3:4])
        self.chsv['date_reported'] = self.chsv.ms.apply(lambda x: x[8:12] + x[5:7])
        self.chsv['name_member'] = self.chsv.ms.apply(lambda x: x[13:33])
        self.chsv['telephone_area_code'] = self.chsv.ms.apply(lambda x: x[34:37])
        self.chsv['telephone_number'] = self.chsv.ms.apply(lambda x: x[38:46])
        self.chsv['extension'] = self.chsv.ms.apply(lambda x: x[47:51])
        self.chsv['member_number'] = self.chsv.ms.apply(lambda x: x[52:62])
        self.chsv['date_opened'] = self.chsv.ms.apply(lambda x: x[66:70] + x[63:65])
        self.chsv['amount'] = self.chsv.ms.apply(lambda x: x[80:95])
        self.chsv['type_account'] = self.chsv.ms.apply(lambda x: x[96:97])
        self.chsv['narrative_code_1'] = self.chsv.ms.apply(lambda x: x[98:100])
        self.chsv['status_code'] = self.chsv.ms.apply(lambda x: x[101:102])
        self.chsv['nsf_information'] = self.chsv.ms.apply(lambda x: x[103:118])
        self.chsv['account_number'] = self.chsv.ms.apply(lambda x: x[119:134])
        self.chsv['segment_code'] = self.chsv.ms.apply(lambda x: x[:2])
        self.chsv['segment_description'] = self.chsv.segment_code.apply(lambda x: seg_abbr_dict[x])
        self.chsv['order_in_segment'] = self.chsv.groupby(['bus_ptnr', 'segment_code']).cumcount()+1
       
        self.chsv.drop(columns=['ms'], inplace=True)      
        self.data.drop(columns=['idx_CS', 'count', 'ms_list', 'filter_nan'], inplace=True)
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
        self.data = self.header.copy()
        # self.data['idx_FB'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" FB ", x)])
        self.data['idx_FI'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" FI ", x)])
        self.data['count'] = self.data.idx_FI.apply(len)
        self.data['ms_list'] = self.data.apply(lambda x: parse_mfile(x, ['idx_FI']), axis=1)
        self.data['filter_nan'] = self.data.ms_list.apply(len)
        self.data = self.data.loc[self.data.filter_nan > 0]
   
        self.frbr['ms'] = self.data['ms_list'].explode().reset_index(drop=True)
        self.frbr['bus_ptnr'] = self.data['business_partner_id'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.frbr['file_date'] = self.data['file_date'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.frbr['date_inquiry'] = self.frbr.ms.apply(lambda x: x[3:13])
        self.frbr['city_narrative'] = self.frbr.ms.apply(lambda x: x[14:32])
        self.frbr['province_narrative'] = self.frbr.ms.apply(lambda x: x[33:53])
        self.frbr['segment_code'] = self.frbr.ms.apply(lambda x: x[:2])
        self.frbr['segment_description'] = self.frbr.segment_code.apply(lambda x: seg_abbr_dict[x])
        self.frbr['order_in_segment'] = self.frbr.groupby(['bus_ptnr', 'segment_code']).cumcount()+1
       
        self.frbr.drop(columns=['ms'], inplace=True)      
        self.data.drop(columns=['idx_FI', 'count', 'ms_list', 'filter_nan'], inplace=True)
        self.frbr['date_inquiry'] = self.frbr.date_inquiry.apply(Converters.convert_8digits_date)
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
        self.data = self.header.copy()
        self.data['idx_LO'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" LO ", x)])
        self.data['count'] = self.data.idx_LO.apply(len)
        self.data['ms_list'] = self.data.apply(lambda x: parse_mfile(x, ['idx_LO']), axis=1)
        self.data['filter_nan'] = self.data.ms_list.apply(len)
        self.data = self.data.loc[self.data.filter_nan > 0]
   
        self.lssv['ms'] = self.data['ms_list'].explode().reset_index(drop=True)
        self.lssv['bus_ptnr'] = self.data['business_partner_id'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.lssv['file_date'] = self.data['file_date'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.lssv['date_reported'] = self.lssv.ms.apply(lambda x: x[3:10])
        self.lssv['name_member'] = self.lssv.ms.apply(lambda x: x[11:31])
        self.lssv['telephone_area_code'] = self.lssv.ms.apply(lambda x: x[32:35])
        self.lssv['telephone_number'] = self.lssv.ms.apply(lambda x: x[36:44])
        self.lssv['extension'] = self.lssv.ms.apply(lambda x: x[45:49])
        self.lssv['member_number'] = self.lssv.ms.apply(lambda x: x[50:60])
        self.lssv['type_code'] = self.lssv.ms.apply(lambda x: x[61:62])
        self.lssv['segment_code'] = self.lssv.ms.apply(lambda x: x[:2])
        self.lssv['segment_description'] = self.lssv.segment_code.apply(lambda x: seg_abbr_dict[x])
        self.lssv['order_in_segment'] = self.lssv.groupby(['bus_ptnr', 'segment_code']).cumcount()+1
       
        self.lssv.drop(columns=['ms'], inplace=True)      
        self.data.drop(columns=['idx_LO', 'count', 'ms_list', 'filter_nan'], inplace=True)
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
        self.data = self.header.copy()
        self.data['idx_IQ'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" IQ ", x)])
        self.data['count'] = self.data.idx_IQ.apply(len)
        self.data['ms_list'] = self.data.apply(lambda x: parse_mfile(x, ['idx_IQ']), axis=1)
        self.data['filter_nan'] = self.data.ms_list.apply(len)
        self.data = self.data.loc[self.data.filter_nan > 0]
   
        self.inqr['ms'] = self.data['ms_list'].explode().reset_index(drop=True)
        self.inqr['bus_ptnr'] = self.data['business_partner_id'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.inqr['file_date'] = self.data['file_date'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.inqr['date_inquiry'] = self.inqr.ms.apply(lambda x: x[3:13])
        self.inqr['name_member'] = self.inqr.ms.apply(lambda x: x[14:34])
        self.inqr['telephone_area_code'] = self.inqr.ms.apply(lambda x: x[35:38])
        self.inqr['telephone_number'] = self.inqr.ms.apply(lambda x: x[39:47])
        self.inqr['extension'] = self.inqr.ms.apply(lambda x: x[48:52])
        self.inqr['member_number'] = self.inqr.ms.apply(lambda x: x[53:63])
        self.inqr['segment_code'] = self.inqr.ms.apply(lambda x: x[:2])
        self.inqr['segment_description'] = self.inqr.segment_code.apply(lambda x: seg_abbr_dict[x])
        self.inqr['order_in_segment'] = self.inqr.groupby(['bus_ptnr', 'segment_code']).cumcount()+1
       
        self.inqr.drop(columns=['ms'], inplace=True)      
        self.inqr['date_inquiry'] = self.inqr.date_inquiry.apply(Converters.convert_8digits_date)
        self.data.drop(columns=['idx_IQ', 'count', 'ms_list', 'filter_nan'], inplace=True)  
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
        self.data = self.header.copy()
        self.data['idx_CD'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" CD ", x)])
        self.data['count'] = self.data.idx_CD.apply(len)
        self.data['ms_list'] = self.data.apply(lambda x: parse_mfile(x, ['idx_CD']), axis=1)
        self.data['filter_nan'] = self.data.ms_list.apply(len)
        self.data = self.data.loc[self.data.filter_nan > 0]
   
        self.csdc['ms'] = self.data['ms_list'].explode().reset_index(drop=True)
        self.csdc['bus_ptnr'] = self.data['business_partner_id'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.csdc['file_date'] = self.data['file_date'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.csdc['date_reported'] = self.csdc.ms.apply(lambda x: x[6:10] + x[3:5])
        self.csdc['date_purged'] = self.csdc.ms.apply(lambda x: x[14:18] + x[11:13])
        self.csdc['declaration'] = self.csdc.ms.apply(lambda x: x[19:79])
        self.csdc['declaration_continued_1'] = self.csdc.ms.apply(lambda x: x[80:158])
        self.csdc['declaration_continued_2'] = self.csdc.ms.apply(lambda x: x[160:238])
        self.csdc['declaration_continued_3'] = self.csdc.ms.apply(lambda x: x[240:318])
        self.csdc['declaration_continued_4'] = self.csdc.ms.apply(lambda x: x[320:398])
        self.csdc['declaration_continued_end'] = self.csdc.ms.apply(lambda x: x[400:428])
        self.csdc['segment_code'] = self.csdc.ms.apply(lambda x: x[:2])
        self.csdc['segment_description'] = self.csdc.segment_code.apply(lambda x: seg_abbr_dict[x])
        self.csdc['order_in_segment'] = self.csdc.groupby(['bus_ptnr', 'segment_code']).cumcount()+1
       
        self.csdc.drop(columns=['ms'], inplace=True)      
        self.data.drop(columns=['idx_CD', 'count', 'ms_list', 'filter_nan'], inplace=True)  
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
        self.data = self.header.copy()
        self.data['idx_BS'] = self.data.mfile.apply(lambda x: [m.start() + 1 for m in re.finditer(" BS ", x)])
        self.data['count'] = self.data.idx_BS.apply(len)
        self.data['ms_list'] = self.data.apply(lambda x: parse_mfile(x, ['idx_BS']), axis=1)
        self.data['filter_nan'] = self.data.ms_list.apply(len)
        self.data = self.data.loc[self.data.filter_nan > 0]
   
        self.busc['ms'] = self.data['ms_list'].explode().reset_index(drop=True)
        self.busc['bus_ptnr'] = self.data['business_partner_id'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.busc['file_date'] = self.data['file_date'].loc[self.data.index.repeat(self.data['count'])].reset_index(drop=True)
        self.busc['product_score'] = self.busc.ms.apply(lambda x: x[3:8])
        self.busc['first_reason_code'] = self.busc.ms.apply(lambda x:  x[9:11])
        self.busc['second_reason_code'] = self.busc.ms.apply(lambda x: x[12:14])
        self.busc['third_reason_code'] = self.busc.ms.apply(lambda x: x[15:17])
        self.busc['fourth_reason_code'] = self.busc.ms.apply(lambda x: x[18:20])
        self.busc['reject_message_code'] = self.busc.ms.apply(lambda x: x[21:22])
        self.busc['reserved'] = self.busc.ms.apply(lambda x: x[26:28])
        self.busc['product_identifier'] = self.busc.ms.apply(lambda x: x[77:79])
        self.busc['segment_code'] = self.busc.ms.apply(lambda x: x[:2])
        self.busc['segment_description'] = self.busc.segment_code.apply(lambda x: seg_abbr_dict[x])
        self.busc['order_in_segment'] = self.busc.groupby(['bus_ptnr', 'segment_code']).cumcount()+1
       
        self.busc.drop(columns=['ms'], inplace=True)      
        self.busc['product_score'] = self.busc.product_score.apply(Converters.convert_int_with_missing).astype('Int64')
        self.data.drop(columns=['idx_BS', 'count', 'ms_list', 'filter_nan'], inplace=True)  
        self._push_seg_table(table=self.busc, table_len=len(self.busc),  seg_name='31_bureau_score', schema=busc_scheme)    def __init__(self, begin_year: int, begin_month: int, 
                 

# This is designed as in a monthly running frequency.
# Each time running this code, designate the year and the month of the data want to be retrieved
# Steps:
#     1. object instantiation with year and month
#     2. call push_tables_to_google_bigquery()
parser = FFFParser(begin_year=2020, begin_month=6)
parser.push_tables_to_google_bigquery()