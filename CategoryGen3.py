import numpy as np
import pandas as pd

class CTGR3:
    def __init__(self, cr_dict):
        self.cr_dict = cr_dict
    '''
    level hunting time을 통해 hunting 구간 설정 및 1초 간격 보간
    '''
    def hunting_section_cut(self, hunt_time):
        hunt_time = hunt_time[::-1]
        sep_idxx =[] # hunting 구간 분리
        for i in range(len(hunt_time)-1):
            current_time = hunt_time[i]
            next_time = hunt_time[i+1]
            time_diff = next_time - current_time 
            if time_diff.total_seconds() > 100:
                sep_idxs.append(i+1)
        # 분리 기준대로 구간 자르기 
        split_timestamps = []
        start_index =0
        for idx in sep_idxs:
            split_timestamps.append(hunt_time[start_index:idx])
            start_index = idx 
        split_timestamps.append(hunt_time[start_index:])
        # 자른 구간별로 1초간격 보간 
        inter_hunt = pd.DataFrame()
        for sub_list in split_timestamps:
            if len(sub_list)!=0:
                temp = (
                    pd.DataFrame({'TIME': sub_list, 'HUNT':1})
                    .set_index('TIME')
                    .resample('1S').bfill()
                )
                inter_hunt = pd.concat([inter_hunt, temp])
                
        return inter_hunt
    '''
    차압식 hunting 계산 
    '''
    def level_hunting(self, data, cr):
        if '차압식' in data.columns:
            restored = (
                data
                .loc[lambda x: x['차압식'].diff() !=0, '차압식']  # 1초 보간 복원 
                .reset_index()
            )
            cut_time = restored.iloc[0,0] + pd.timedelta(minutes = cr.get('dpl1')) # 처음부터 40분까지 제외 
            cut_idx = restored['TIME'].sub(cut_time).abs().idxmin()
            rest_cut = restored.iloc[cut_idx:]
            rest_cut_len = len(rest_cut)
            
            hunt_time =[]
            for k in range(rest_cut_len):
                end_40 = rest_cut.iloc[rest_cut_len-1-k, 0] # 40분 end time
                str_40 = end_40 - pd.Timedelta(ninutes = cr.get('dpl1'))  # 40분 start time 
                end_ix = restored.loc[lambda x: x['TIME']==end_40].index.values[0]
                if (restored['TIME']==str_40).sum() > 0:    # start time과 같은 time이 존재한다면, 
                    str_idx = restored.loc[lambda x: x['TIME']==str_40].index.values[0]
                else:  # start time과 같은 time이 존재하지 않으면 가장 가까운 값으로 
                    str_idx = restored['TIME'].sub(str_40).abs().idxmin()
                if str_idx = end_idx: str_idx = str_idx -1 

                check_range = restored.iloc[str_idx:end_idx]
                if check_range.shape[0]!=0:
                    cr= check_range.iloc[-1, 1]  # 맨 마지막 차압식
                    if abs(cr - check_range.iloc[:-1, 1].min()) > cr.get('dpl2'):
                        hunt_time.append(check_range.iloc[-1, 0])

            if (hunt_time ==[]):
                data_hunt = data.copy()
                data_hunt['HUNT'] =0
            else:
                inter_hunt = self.hunting_section_cut(hunt_time)
                data_hunt = data.join(inter_hunt, how='left') 
        else:
                data_hunt = data.copy()
                data_hunt['HUNT'] =0
            
        return data_hunt 
    '''
    카테고리 생성  
    '''
    def init_category_gen(self, df):
        for rule_id, cr in self.cr_dict.items():
            cat_df = rule_id.split('-')[0]
            if cat_id =='D_TD': 
                if ('액온도1' in df.columns) and ('액온도2' in df.columns):
                    c1 = df['ProcValve']==cr.get('pv')
                    c2 = (df['전류'] >= cr.get('amp1')) & (df['전류'] <= cr.get('amp2'))
                    c3 = (df['액온도1'] < cr.get('tz1')) | (df['액온도2'] < cr.get('tz2'))
                    df[rule_id] = np.where(c1 & c2 &c3, 1, 0)
                    
            elif cat_id =='D_TI':
                if ('액온도1' in df.columns) and ('액온도2' in df.columns):
                    c1 = df['ProcValve']==cr.get('pv')
                    c2 = (df['전류'] >= cr.get('amp1')) & (df['전류'] <= cr.get('amp2'))
                    c3 = (df['액온도1'] > cr.get('tz1')) | (df['액온도2'] > cr.get('tz2'))
                    df[rule_id] = np.where(c1 & c2 &c3, 1, 0)
                    
            elif cat_id =='B_CWO':
                c = df['AmpSumm'] < cr.get('ampsum')
                c1 = df['ProcValve']==cr.get('pv')
                c2 = df['전류'] < cr.get('amp')
                c3 = df['CW_CV'] > cr.get('cw')
                df[rule_id] = np.where(c &c1 & c2 &c3, 1, 0)            
                
            elif cat_id =='B_LLU':
                c = df['AmpSumm'] < cr.get('ampsum')
                c1 = df['전류'] < cr.get('amp')
                c2 = df['ProcValve']==cr.get('pv')
                if ('높이L' in df.columns) and ('높이LL' in df.columns):
                    c3 = (df['높이L'] == cr.get('lv1')) | (df['높이LL'] == cr.get('lv2'))
                    df[rule_id] = np.where(c &c1 & c2 &c3, 1, 0)     
                else: df[rule_id] = np.nan
                    
            elif cat_id =='B_TD': 
                if ('액온도1' in df.columns) and ('액온도2' in df.columns):
                    c =  df['AmpSumm'] < cr.get('ampsum')
                    c1 = df['전류'] < cr.get('amp')
                    c2 = df['ProcValve']==cr.get('pv')
                    c3 = (df['액온도1'] < cr.get('tz1')) | (df['액온도2'] < cr.get('tz2'))&(pd.isna(df['액온도2'])==False))
                    df[rule_id] = np.where(c& c1 & c2 &c3, 1, 0)          
                    
            elif cat_id =='B_TI': 
                if ('액온도1' in df.columns) and ('액온도2' in df.columns):
                    c =  df['AmpSumm'] < cr.get('ampsum')
                    c1 = df['전류'] < cr.get('amp')
                    c2 = df['ProcValve']==cr.get('pv')
                    c3 = (df['액온도1'] > cr.get('tz1')) | (df['액온도2'] > cr.get('tz2'))
                    df[rule_id] = np.where(c& c1 & c2 &c3, 1, 0)          
                    
            elif cat_id =='B_PLH': 
                c1 = df['AmpSumm'] < cr.get('ampsum')
                c2 = df['전류'] < cr.get('amp')
                data = df[c1&c2]
                hunt_df = self.level_hunting(data, cr)
                df = df.join(hunt_df[['HUNT']]).rename(columns={'HUNT': rule_id})
                
            elif cat_id =='E_TD': 
                if ('액온도1' in df.columns) and ('액온도2' in df.columns):
                    c =  df['AmpSumm'] >= cr.get('ampsum')
                    c1 = df['전류'] < cr.get('amp')
                    c2 = df['ProcValve']==cr.get('pv')
                    c3 = (df['액온도1'] < cr.get('tz1')) | (df['액온도2'] < cr.get('tz2'))&(pd.isna(df['액온도2'])==False))
                    df[rule_id] = np.where(c& c1 & c2 &c3, 1, 0)          
                    
            elif cat_id =='E_TI': 
                if ('액온도1' in df.columns) and ('액온도2' in df.columns):
                    c =  df['AmpSumm'] >= cr.get('ampsum')
                    c1 = df['전류'] < cr.get('amp')
                    c2 = df['ProcValve']==cr.get('pv')
                    c3 = (df['액온도1'] > cr.get('tz1')) | (df['액온도2'] > cr.get('tz2'))
                    df[rule_id] = np.where(c& c1 & c2 &c3, 1, 0)                   
                    
            elif cat_id =='E_LLU': 
                c = (df['AmpSumm'] >= cr.get('ampsum'))
                c1 = df['전류'] < cr.get('amp')
                if ('높이L' in df.columns) and ('높이LL' in df.columns):
                    c2 = (df['높이L']==cr.get('lv1')) | (df['높이LL']==cr.get('lv2'))
                    df[rule_id] = np.where(c&c1&c2, 1, 0)
                else:
                    df[rule_id] = np.nan
            
            elif cat_id =='E_PLH': 
                c1 = df['AmpSumm'] >= cr.get('ampsum')
                c2 = df['전류'] < cr.get('amp')
                data = df[c1&c2]
                hunt_df = self.level_hunting(data, cr)
                df = df.join(hunt_df[['HUNT']]).rename(columns={'HUNT': rule_id})
                
        return df 



        
    
