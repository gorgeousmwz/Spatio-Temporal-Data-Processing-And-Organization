from pymongo import MongoClient
import geohash

#查询县级行政区的geohash编码文件
def select_geohash(names):
    #建立连接指定数据库集合
    client=MongoClient('localhost',27017)
    db=client.test
    collection=db.mawenzhuo
    #处理输入
    names=names.split("-")
    #查询指定的县级区域
    condition={}#查询条件
    for i in range(len(names)):#每一级行政区名字都要吻合
        condition["properties.NAME_"+str(i)]=names[i]
    ans=collection.find_one(condition,{"_id":0,"properties":1,"geometry":1})
    #得到geohash编码
    code=''
    for point in ans["geometry"]["coordinates"][0]:
        #调用自己编写的geohash模块函数,获取每个点的8位geohash编码
        code=code+geohash.get_geohash(point[0],point[1],8)
    return code
    


names=input('请输入想要查询的县级行政区(如China-Hubei-Ezhou-Echeng Shì):')
code=select_geohash(names)
print('geohash编码为:')
print(code)
