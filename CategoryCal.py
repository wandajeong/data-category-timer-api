from CategoryGen3 import CTGR3
from CategoryGen4 import CTGR4
import pandas as pd 

class CTGRcal:
    def __init__(self, dt_df, cr_dict, p_no):
        self.dt_df = dt_df
        self.cr_dict = cr_dict
        self.p_no = p_no
        self.cols = ['col1', 'col2', 'col3',] # 제외 대상 columns 
        
    def track_stmp(self, df):
        st_times, ed_times, ttl_secs =[], [], []
        tmp = (
            pd.DataFrame(df[item])
            .assign(flag= lambda x: x[item]==1)
            .assign(group= lambda x: (x['flag'] != x['flag'].shift()).cumsum())
        )
        groups = tmp[tmp['flag']].groupby('group')
        for _, group in groups:
            st_times.append(group.index[0])
            ed_times.append(group.index[-1])
            tt_sec =  (group.index[-1] - group.index[0]).total_seconds() +1 
            ttl_secs.append(tt_sec)
            
        track_df = pd.DataFrame({'CTGR': item, 'START': st_times, 'END': ed_times, 'SECONDS': ttl_secs})
        track_df_con = pd.concat([track_df_con, track_df])

        return track_df_con 
        
    def get_init_ctgr(self, init_cr):
        if self.p_no ==3: cg= CTGR3(init_cr)
        else: cg= CTGR4(init_cr)
            
        init_cate_df = cg.init_category_gen(self.dt_df)
        for col in self.cols:
            if col in init_cate_df.columns:
                init_cate_df = init_cate_df.drop(columns=col)

        trb_df = self.track_stmp(init_cate_df)
        return trb_df 
        
    def get_op_ctgr(self, op_cr):
        if self.p_no ==3: cg= CTGR3(op_cr)
        else: cg= CTGR4(op_cr)
            
        op_cate_df = cg.init_category_gen(self.dt_df)
        for col in self.cols:
            if col in op_cate_df.columns:
                op_cate_df = op_cate_df.drop(columns=col)
                
        tre_df = self.track_stmp(op_cate_df)
        return tre_df 
        
    def cate_result(self):
        cate_sum = pd.DataFrame()
        if self.dt_df.shape[0]==0: cate_sum = pd.DataFrame()
        else:
            if all(col in self.dt_df.columns for col in ['AmpSumm', 'ProcValve', '전류']):
                if self.p_no ==3: cg= CTGR3(self.cr_dict)
                else: cg= CTGR4(self.cr_dict)
                    
                cate_df = cg.category_gen(self.dt_df)
                for col in self.cols:
                    for col in cate_df.columns:
                        cate_df = cate_df.drop(columns = col)
                cate_sum = self.track_stmp(cate_df)
            else:
                cate_sum= pd.DataFrame()
                
        return cate_sum





        
