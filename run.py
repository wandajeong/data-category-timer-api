import pandas as pd
import argparse
import json
from tqdm import tqdm
from DBconn import FetchDB
from GetData import getData
from CategoryGen3 import CTGR3
from CategoryGen4 import CTGR4 
from pathlib import Path

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

def table_insert(res_df, op_hour):
    """
    결과 데이터를 분기(OP_CAT, OPCAT_SUM)하여 각 테이블에 DATA INSERT
    """
    res_df = res_df.drop_duplicates(keep='first')
    opcat_df = (
        res_df
        .drop(['DURATION'], axis=1)[['RAID', 'RULEID', 'START_TIME', 'END_TIME']]
    )
    opcat_sum_df = (
        res_df
        .groupby(['RAID', 'RULEID'])
        .agg({'DURATION': 'sum'})
        .assign(OCCUR_TIME= lambda x: (x['DURATION']/(op_hour*3600))*100)
        .reset_index()
    )
    output1 = dbobj.insert_data(opcat_df, issum=0)
    output2 = dbobj.insert_data(opcat_sum_df, issum=1)
    
    return output1, output2 

def main(db, cr_dict):
    tag_info, raid = get_meta(db)
    cate_results = pd.DataFrame()
    
    for i, (_, row) in tqdm(enumerate(raid.iterrows()), total=raid.shape[0]):
        code = str(row.CODE)
        if row.PLANT_ID =='P04': p_no = 4
        else: p_no = 3 
        
        path = Path.cwd() / f'p0{p_no}'
        path1 = path + code 

        str_time, end_time = row.OP_START, row.OP_END
        dt_list = pd.date_range(str_time.date(), end_time.date(), freq='D').strftime('%Y%m%d').tolist()
        gd= getData(tag_info, path1, dt_list)
        
        for dt in dt_list:
            try: action, dt_df = gd.get_day_data(dt, str_time, end_time)  # 하루 단위 데이터 
            except UnboundLocalError as e: pass
            if action =='continue': continue
                
            dt_dt = gd.data_organize(dt_df)
            cate_sum = (
                cate_result(dt_df, cr_dict, p_no)
                .assign(RAID = row.RAID)
            )

        output1, output2 = table_insert(cate_results, op_hour = row.OP_HOUR)
        print(output1, output2)

if __name__=='__main__':
    with open(args.input_file, 'r') as f:
        try:
            input_data = json.load(f)
        except json.JSONDecodeError:
            print('{',
                  f'"inst_id" : " ", "Result": "Fail, Not a valid JSON string", "output": -1',
                  '}')   
    main(dbobj, input_data)










        

