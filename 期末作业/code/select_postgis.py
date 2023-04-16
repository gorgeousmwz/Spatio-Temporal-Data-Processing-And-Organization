import psycopg2
import time

#连接数据库
def connect_db():
    try:
        conn=psycopg2.connect(database='test',user='postgres',
        password='090012',host='127.0.0.1',port=5432)
    except Exception as e:
        print(e)
    else:
        return conn
    return None

#关闭数据库
def close_db(cur,conn):
    conn.commit()#事物提交
    cur.close()
    conn.close()#关闭

#查询西经60度的经线穿过的所有国家
def select_db():
    conn=connect_db()#连接数据库test
    if not conn:
        return None
    cur=conn.cursor()#创建cursor对象执行SQL命令
    sql='SELECT NAME_0 from mawenzhuo where ST_Intersects(mawenzhuo.geom,\
                       ST_GeomFromText(\'LINESTRING(-60 -89.99,-60 89.99)\',4326)) group by NAME_0'#SQL语句
    time_start=time.time()
    cur.execute(sql)#查询
    time_end=time.time()
    rows=cur.fetchall()#拉取数据
    print('西经60度经线穿过以下国家:')
    for i in rows:#结果输出
        print(i[0])
    print('耗时:',time_end-time_start,'s')
    close_db(cur,conn)#关闭


select_db()



