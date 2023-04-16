from pymongo import MongoClient
import time

#建立连接，指定数据库集合
client=MongoClient('localhost',27017)
db=client.test
collection=db.mawenzhuo

#进行查询
time_start=time.time()
ans=collection.find({
                    "geometry":{"$geoIntersects":
                    {"$geometry":{"type":"LineString","coordinates":
                    [[-60,-89.99],[-60,89.99]]}}}},
                    {"_id":0,"properties":1})

#结果打印
print('西经60度经线穿过以下国家:')
countrys=[]
for i in ans:
    property=i['properties']
    country=property['NAME_0']
    if country not in countrys:
        countrys.append(country)
for i in countrys:
    print(i)
time_end=time.time()
print('耗时:',time_end-time_start,'s')