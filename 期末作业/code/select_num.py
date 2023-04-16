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
    conn.close()#关闭

#统计国家县级行政区数量并输出面积做大的三个
def select_num(country_name):
    conn=connect_db()#连接数据库
    if not conn:
        return None
    cur=conn.cursor()#创建游标对象
    #sql语句查询每个国家的县级行政区数量
    sql1="SELECT COUNT(*) from mawenzhuo where NAME_0 like '%"+country_name+"%'"
    cur.execute(sql1)#执行查询
    num=cur.fetchall()#抓取数据
    #sql语句查询这个国家的县级行政区名称、面积
    sql2="SELECT NAME_3,ST_AREA(ST_TRANSFORM(mawenzhuo.geom,4527)) FROM mawenzhuo \
        WHERE NAME_0 like '%"+country_name+"%' \
        ORDER BY ST_AREA(ST_TRANSFORM(mawenzhuo.geom,4527)) DESC"
    cur.execute(sql2)
    area=cur.fetchall()
    print(country_name+'包含县级行政区数目为:',num[0][0])
    print(country_name+'中面积前三的县级行政区依次如下:')
    for i in range(3):#输出前三大的区域
        print('The area of '+area[i][0]+' is '+str(area[i][1])+' m2')
    close_db(cur,conn)#关闭数据库


country_name=input('请输入想要查询的国家名称（英文）：')
select_num(country_name)
