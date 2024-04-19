import pymssql

class FetchDB:
  def __init__(self):
    self.serverip = "server_1"
    self.user = "username1"
    self.password = "password"
    self.database = "DB1"

  def sql_connect(self):
    conn = pymssql.connect(
      server = self.serverip, user = self.user, password = self.password, database = self.database
    )
    cursor = conn.cursor()
    return conn, cursor

  def insert_data(self, df, issum):
    """
    예측 결과를 database에 입력
    동일한 RAID와 RULEID가 db에 있으면 SKIP, 없으면 INSERT
    return: 2 (insert 성공) / -1 ( insert 실패 )
    """
    result_tuples = [tuple(row) for row in hsum_df.values]
    col_str = 'RAID, RULEID, START_TIME, END_TIME'
    if issum: table = "TABLE_NAME_SUM"
    else: table = "TABLE_NAME"
    
    conn, cursor = self.sql_connect()

    try:
    	for record in result_tuples:
        raid = record[0]
        ruleid = record[1]

        query_check = f"""SELECT * FROM {table} 
                    WHERE RAID = %s AND RULE_ID = %s"""
        cursor.execute(query_check, (raid, ruleid))
        existing_record = cursor.fetchone()
      
        if existing_record: continue
        else:
          query = f"""INSERT INTO {table} VALUES (%s, %s, %s, %s)"""
          cursor.execute(query, record)
          conn.commit()

      output_param = 2
    except Exception as e:
      output_param = -1
    
    conn.close()
    return output_param
