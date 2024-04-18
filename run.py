import pandas as pd
import argparse
import json
from tqdm import tqdm
from DBconn import FetchDB
from GetData import getData
from CategoryGen3 import CTGR3
from CategoryGen4 import CTGR4 

parser = argparse.ArgumentParser()
parser.add_argument("--input_file", requred=True, help="JSON input file")
args =parser.parse_args()

dbobj = FetchDB()

def get_meta(db):
    conn, cursor = db.sql_connect()

    query1 = "SELECT CODE, TAG_ID, TAG_DESC FROM TAG"
    tags = pd.read_sql(sql=query1, con=conn)

    query2 = """SELECT RD.*, EQ.PLANT_ID FROM RAID_GS AS RD
            JOIN EQ_INFO AS EQ ON EQ.CODE = CAST(RD.CODE AS VARCHAR)"""
    raid = pd.read_sql(sql=query2, con=conn)
    tag_info = (
        tags.loc[tags['CODE'].str.startswith('1')]
        .loc[lambda x: x['CODE'].astype('int')
        .reset_index(drop=True)
    )
    return tag_info, raid

def track_stmp(df):
    """
    카테고리 발생 구간(시작-끝) TRACKING
    """
    cal_times = df.sum()[df.sum()!=0].index.tolist()
    track_df_con = pd.DataFrame()

    for item in cal_items:
        st_times, ed_times, ttl_secs =[], [], []
        tmp = pd.DataFrame(df[item])
        tmp['flag'] = tmp[item]==1
        tmp['group'] = (tmp['flag'] != tmp['flag'].shift()).cumsum()
        groups = tmp[tmp['flag']].groupby('group')
        for _, group in groups:
            st_times:append(group.index[0])
            ed_times.append(group.index[-1])
            tt_sec = (group.index[-1] - group.index[0]).total_seconds() +1 
            ttl_secs.append(tt_sec)

        track_df = pd.DataFrame(
            {'RULEID': item, 'START_TIME': st_times, 'END_TIME': ed_times, 'DURATION': ttl_secs}
        )
        track_df_con = pd.concat([track_df_con, track_df])

    return track_df_con 

def cate_result(dt_df, cr_dict, p_no):
    """
    하루치 데이터에 대해 카테고리 생성 후, 
    카테고리 발생 구간 결과 출력
    """
    cate_sum =pd.DataFrame()
    cols = ['전류', 'AmpSumm', '차압식', '높이L', '높이LL', 'CW_CV', '액온도1', 'ProcValve', '액온도2']
    
    if dt_df.shape[0] ==0: cate_sum = pd.DataFrame()
    else:
        if all(col in dt_df.columns for col in ['AmpSumm', 'ProcValve', '전류']):
            if p_no==3: cg = CTGR3(cr_dict)
            else: cg = CTGR4(cr_dict)
                
            cate_df = cg.category_gen(dt_df)
            for col in cols: 
                if col in cate_df.columns:
                    cate_df = cate_df.drop(columns=col)

        else:
            cate_sum = pd.DataFrame()
            
    return cate_sum





