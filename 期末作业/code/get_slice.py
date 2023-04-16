import psycopg2
from PIL import Image
from PIL import ImageDraw
import random
import json

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

#经纬坐标转像素坐标
def transform(longitude,latitude,width,height,minx,miny,ratio):
    #longitude,latitude经纬度坐标
    #width，height图像宽高
    #minx，miny图像左下角经纬度坐标
    #ratio分辨率
    col=int((longitude-minx)/ratio+0.5)#计算列号
    row=int(height-(latitude-miny)/ratio+0.5)#计算行号
    return row,col#返回行列号

#查询
def select(sql):
    conn=connect_db()#连接数据库
    cur=conn.cursor()#生成游标对象
    cur.execute(sql)#执行查询
    ans=cur.fetchall()#抓取数据
    close_db(cur,conn)#关闭数据库连接
    return ans

#对指定国家数据按照指定分辨率进行渲染，对渲染后的栅格进行切分，输出文件
def select_slice(country_name,ratio,target_path): 
    #sql语句查询指定国家每条数据的最大最小经纬坐标
    sql="SELECT ST_ASGEOJSON(geom),ST_XMIN(geom),ST_XMAX(geom),ST_YMIN(geom),ST_YMAX(geom)\
        FROM mawenzhuo WHERE NAME_0 like '%"+country_name+"%'"
    ans=select(sql)
    #确定外接矩形大小
    #定义初始坐标最大最小值
    minx=180
    maxx=-180
    miny=90
    maxy=-90
    #循环更新边界范围,并存储边界点坐标
    points_list=[]#存储经纬坐标
    for i in ans:#循环更新范围
        if i[1]<minx:
            minx=i[1]
        if i[2]>maxx:
            maxx=i[2]
        if i[3]<miny:
            miny=i[3]
        if i[4]>maxy:
            maxy=i[4]
        #存储边界坐标
        temp = json.JSONDecoder().decode(i[0])#解码
        points_list.append(temp["coordinates"][0][0])#存储
    print('经纬度范围:',minx,maxx,miny,maxy)
    #根据边界范围确定图片大小
    width=int((maxx-minx)/ratio)
    height=int((maxy-miny)/ratio)
    #创建图片
    img=Image.new("RGB",(width+160,height+160),"white")
    #转换为像素坐标
    for points in points_list:
        pixels=[]#存储像素坐标
        for point in points:
            #坐标转换
            py,px=transform(point[0],point[1],width,height,minx,miny,ratio)
            pixels.append((px,py))#加入像素点
        #绘制多边形
        ImageDraw.Draw(img).polygon(pixels,#逐点绘制
                            outline='rgb(0,0,0)',#用黑色连线
                            fill=f'''rgb({random.randint(0,235)},{random.randint(0,235)},{random.randint(0,235)})''')#随机颜色填充
    img.save(target_path+'result5.jpg')

    #进行切片
    num_x=int(width/256)+1#按照256x256的网格进行切片
    num_y=int(height/256)+1
    for i in range(num_x):
        for j in range(num_y):
            x1,y1,x2,y2=256*i,256*j,256*(i+1),256*(j+1)
            img_slice=img.crop((x1,y1,x2,y2))#按格网切分
            img_slice.save(target_path+'slice\\'+str(j+1)+'-'+str(i+1)+'.jpg')#存储
            #生成对应的jgw文件
            with open(target_path + 'slice\\'+str(j+1)+'-'+str(i+1)+'.jgw','w+', encoding='UTF-8') as jgw:
                jgw.write(f'{ratio}\n'#x方向比例因子
                          f'{0}\n'#旋转项
                          f'{0}\n'#旋转项
                          f'{-ratio}\n'#y方向比例因子
                          f'{minx +256 * i * ratio}\n'#左上角经度
                          f'{maxy - 256 * j * ratio}')#左上角维度
    
country_name=input('请输入想要查询的国家名称（英文）:')
ratio=input('分辨率:')
select_slice(country_name,float(ratio),"D:\\本科\\时空数据处理与组织\\期末设计\\code\\result\\")