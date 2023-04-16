#coding=utf-8
import findspark
findspark.init()

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, BooleanType, FloatType, StringType
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import math

def read_data():#数据读取
    spark=SparkSession.builder.config(conf=SparkConf()).getOrCreate()#创建SparkSession对象
    #读入数据，存储到Rdd中
    data_rdd=spark.sparkContext\
        .textFile('.\\data\\Geolife Trajectories 1.3\\Data\\170\\Trajectory\\20080428112704.plt')
    #数据处理（除去表头和异常数据，按逗号为分隔符处理数据,数值化,转为DataFrame）
    data=data_rdd.filter(lambda line:len(line.split(','))==7)\
            .map(lambda line:line.split(','))\
            .zipWithIndex()\
            .map(lambda line : Row(id = int(line[1]), latitude = float(line[0][0]),
            longitude = float(line[0][1]), time= float(line[0][4]),time_str=line[0][6]))\
            .toDF()
    return data

def get_time(time1,time2):#获取时间间隔
    return 24.0*60.0*60.0*(time2-time1)#单位是s

def get_distance(lat1,lon1,lat2,lon2):#计算两经纬坐标距离
    R = 6371.393#地球平均半径
    Pi = math.pi#圆周率
    #Haversine公式
    a = (math.sin(math.radians(lat1/2-lat2/2)))**2
    b = math.cos(lat1*Pi/180) * math.cos(lat2*Pi/180) * (math.sin((lon1/2-lon2/2)*Pi/180))**2
    L = 2 * R * math.asin((a+b)**0.5)#单位是千米
    return 1000*L#单位为m

def get_speed(id,lat1,lon1,lat2,lon2,time1,time2):#计算一点的即时速度
    if id==int(0) or id==int(num-1):#如果是第一行或者最后一行
        return 0.0#速度为0
    time=get_time(time1,time2)#获取时间间隔，单位为s
    distance=get_distance(lat1,lon1,lat2,lon2)#获取距离，单位为千米
    speed=distance/time#计算速度=距离/时间间隔(m/s)
    return speed

stop_id=0#停留点序号
threshold=1.0#速度阈值设为1m/s
def is_stoppoint(id,speed1,speed2):#标记是否为停留点
    #speed1为当前时刻速度,speed2下一时刻速度,threshold为停留阈值
    global stop_id
    if id==int(num-1):#如果是最后一行
        if(speed1<threshold):
            return int(stop_id)
        else:
            return int(0)
    if(speed1<threshold):#当前时刻速度小于阈值一定是停留点
        return int(stop_id)
    elif(speed2<threshold):#下一时刻速度小于阈值也是停留开始点
        stop_id=stop_id+1#停留点+1
        return int(stop_id)
    else:#其他情况
        return int(0)

def get_acceleration(id,speed1,speed2,time1,time2):#获取相邻两点间加速度
    if id==int(0):#如果是第一行
        return 0.0#加速度为0
    time=get_time(time1,time2)
    d_speed=speed2-speed1#速度之差
    return d_speed/time#速度增量除时间=加速度(m/s2)

def spot_state(id,stop_point,a1,a2):#判断运动状态
    #这里取N=3，即要在至少后面N-1个点速度单调变化才可
    #id为点号，stop_point为停留点序号
    #a1为当前点到下一点的加速度，a2为下一点到下下点的加速度
    if id==num-1:#最后一个点时
        return 'destination'
    if stop_point>0:#如果是停留点
        return 'stop'
    else:#如果不是停留点
        if id==num-2:#倒数第二点时
            if a1>0:
                return 'accelerate'
            if a1<0:
                return 'decelerate'
        if a1>0 and a2>0:
            return 'accelerate'#加速
        if a1<0 and a2<0:
            return 'decelerate'#减速
        else:
            return 'tansition'#过渡
        


#读取数据(此时数据处理已经完成),格式是DataFrame
data=read_data()
#创建窗口
my_window = Window.partitionBy().orderBy("id")#不分组，按id排序
#计算每一个点的即时速度
num=data.count()#数据行数
GET_SPEED=udf(get_speed,FloatType())
data=data.withColumn('speed',GET_SPEED(
        F.col("id"),
        F.lag("latitude",1).over(my_window),F.lag("longitude",1).over(my_window),
        F.col("latitude"),F.col("longitude"),
        F.lag("time",1).over(my_window),F.col("time")
        ))
#停留点判断
GET_STOP=udf(is_stoppoint,IntegerType())
data=data.withColumn('stop_point',GET_STOP(
        F.col('id'),F.col("speed"),F.lead("speed",1).over(my_window)
        ))
#加速度计算
GET_ACC=udf(get_acceleration,FloatType())
data=data.withColumn('acceleration',GET_ACC(
        F.col("id"),
        F.lag("speed",1).over(my_window),F.col("speed"),
        F.lag("time",1).over(my_window),F.col("time")
        ))
#运动状态判断
GET_STATE=udf(spot_state,StringType())
data=data.withColumn("spot_state",GET_STATE(
        F.col('id'),F.col('stop_point'),
        F.lead('acceleration',1).over(my_window),
        F.lead('acceleration',2).over(my_window)
        ))
#保存到csv中
data.coalesce(1).write.option("header", "true").csv(".\\code\\result.csv")
