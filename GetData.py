import numpy as np
import pandas as pd 
import xlwings as xw
import os
import zipfile

class getData:
    def __init__(self. tag_info, path, dt_list):
        self.tag_info = tag_info
        self.path = path
        self.dt_list = dt_list 
    '''
    각 날짜 폴더에서 raw_data 가져오기
    '''
    def get_raw_sec(self, path02, tag_id, zor, dt, my_zip):
        str_idx_list, end_idx_list = [], []
        inter_dict ={}
        d_tags=["col1", "col2", ] ## 제외할 column들 
        
        for tag in tag_id:
            m_idx = self.tag_info.loc[lambda x: tag==x['TAG_ID']].index 
            tag_name = self.tag_info.loc[m_idx, 'TAG_DESC']
            tag_name = str(tag_nmae.values).replace("['",'').replace("']", '').replace(' ', '')

            if (tag_name.startswith('양극')) or tag_name in d_tags:  # 필요한 데이터만 추출 
                continue
            else:
                if zor:
                    data = (
                        my_zip.read(tag+'.dat')
                        .decode('utf-8')
                        .split('\r\n')
                    )
                    data= pd.Series(data).str.split(',', expand=True).iloc[:-1]
                    data.columns = ['DATE', tag_name]
                else:
                    file_path = path02 + '/' + tag + '.dat'
                    data= pd.read_table(file_path, sep=',', names=['DATE', tag_name])
                    
                data = (
                    data
                    .assign(DATE= lambda x: pd.to_datetime(x['DATE'])
                    .drop_duplicates(subset=['DATE'], keep='last')
                    .set_index('DATE', inplace=True)
                )
                str_idx_list.append(data.first_valid_index())
                end_idx_list.append(data.last_valid_index())
                    
                inter_data = data.resample('1S').ffill()    # 1초 단위로 데이터 보간
                inter_dict[tag_name] = inter_data
                
        if len(str_idx_list)==0: str_idx = pd.Timestamp(dt + ' 00:00:00')
        else: str_idx = min(str_idx_list)
            
        if len(end_idx_list)==0: end_idx = pd.Timestamp(dt + ' 23:59:59')
        else: end_idx = min(end_idx_list)     
            
        return str_idx, end_idx, inter_dict
    '''
    dt_list 중 dt(1day) 구간 데이터 추출
    '''
    def get_day_data(self, dt, str_time, end_time):
        path02 = self.path + '/' + dt
        action = None
        global str_idx, end_idx, indtr_dict
        
        if os.path.exist(path02): # 날짜가 있다면 그대로 진행, 없다면 .zip을 붙여서 
            zor = 0
            dat_list = os.listdir(path02)
            tag_id = [dat.strip('.dat') for dat in dat_list]
            str_idx, end_idx, inter_dict = self.get_raw_sec(path02, tag_id, dt, my_zip = None) 
        else:
            zor = 1
            path02 = self.path = '/' + dt + '.zip'
            if os.path.exists(path02):
                my_zip = zipfile.Zipfile(path02, 'r')
                tag_id = [zid.strip('.dat') for zid in my_zip.manelist()]
                str_idx, end_idx, inter_dict = self.get_raw_sec(path02, tag_id, zor, dt, my_zip)
            else: 
                action = 'continue'
                
        new_idx = pd.date_range(str_idx, end_idx, freq='1S')
        dt_df = pd.DataFrame()
        for key, inter_df in inter_dict.items():
            n_df = inter_df.reindex(new_idx).fillna(0)
            dt_df = pd.concat([dt_dt, n_df], axis=1)
            
        dt_df.index.name = 'TIME'
        dt_df = dt_df.reset_index()
        
        # 운전 시작시간과 종료시간에 따라 데이터 cut 
        s_idx = dt_df[dt_df['TIME']==str_time].index
        if len(s_idx)==1:
            dt_df = dt_df.iloc[int(s_idx.values):]
        e_idx = dt_df[dt_df['TIME']==end_time].index
        if len(e_idx)==1:
            dt_df = dt_df.iloc[int(e_idx.values):]
        dt_df = dt_df.set_index('TIME')
        
        return action, dt_df 
        
    '''
    추출 raw 데이터 전처리 
    '''
    def data_organize(self, df):
        df.rename(columns = {'col1': 'AmpSumm', 'col2': 'CW_CV', 'col3': 'ProcValve'}, inplace=True) 
        df.columns = df.columns.str.replace('Cell', '')
        if '액온도' in df.columns:
            df['액온도'] = np.where(df['액온도'] =='System.__error', np.nan, df['액온도'])
        if "전압''전압" in df.columns:
            df.drop(["전압''전압"], axis=1, inplace=True)
        if 'ProcValve' in df.columns: 
            df['ProcValve'] = np.where(df['ProcValve'] =='OPEN', 'ON', np.where(df['ProcValve'] =='CLOSE', 'OFF', df['ProcValve']))
            
        for col in df.columns:
            try:
                df[col] = np.where(df[col]=='', np.nan, df[col])
            except: pass
        # 데이터 type 변경 
        cat_features = ['높이L', '높이LL', 'ProcValue']
        col_to_f = list(set(list(df.columns)) - set(cat_features))
        df[col_to_f] = df[col_to_f].astype('float')

        return df 
                                         
            




