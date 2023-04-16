import psycopg2

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
    conn.close()

#查询输入国家的邻接国家
def select_near(country_name):
    conn=connect_db()#连接数据库
    cur=conn.cursor()#创建游标对象
    sql="SELECT table2.NAME_0 FROM mawenzhuo table2,\
        (SELECT * FROM mawenzhuo where NAME_0 like '%"+country_name+"%') table1 \
        where ST_TOUCHES(table1.geom,table2.geom) \
        and table2.NAME_0 <> '"+country_name+"' \
        GROUP BY table2.NAME_0"
    cur.execute(sql)#查询
    countrys=cur.fetchall()#拉取数据
    #打印结果
    print('与'+country_name+'相邻接的国家一共'+str(len(countrys))+',具体如下:')
    for i in range(len(countrys)):
        print(countrys[i][0])
    close_db(cur,conn)#关闭数据库


country_name=input('请输入想要查询的国家(英文):')
select_near(country_name)