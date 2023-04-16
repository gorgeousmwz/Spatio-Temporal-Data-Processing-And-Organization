import math
#二分
def divide(min,max,data,precision):
    length=0#记录长度
    code=[]#记录编码
    while length<precision:#没有达到精度前循环划分
        mid=(min+max)/2#计算中间值
        if data>=mid:#如果大于等于中间值赋1
            code.append(1)
            length=length+1
            min=mid#更新最小值
        else:#小于中间值赋0
            code.append(0)
            length=length+1
            max=mid#更新最大值
    return code#返回编码

#二进制转十进制
def binary2decimalism(binary):
    decimalism=0
    for i in range(len(binary)-1,-1,-1):
        decimalism=decimalism+binary[i]*2**(len(binary)-i-1)
    return decimalism

#获取经纬度二进制编码
def get_binary(longitude,latitude,precision):
    longitude_binary=divide(-180,180,longitude,math.ceil(5*precision/2))#计算经度二进制编码
    latitude_binary=divide(-90,90,latitude,5*precision//2)#计算维度二进制编码
    code_binary=[]#存储合并的二进制编码
    for i in range(5*precision//2):#合并
        code_binary.append(longitude_binary[i])#经度占偶数位
        code_binary.append(latitude_binary[i])#纬度占奇数位
    if (precision%2)==1:
        code_binary.append(longitude_binary[-1])
    return code_binary

#获取base32编码
def get_base32(code_binary):
    #Base32编码表
    base32='0123456789bcdefghjkmnpqrstuvwxyz'
    #将二进制编码划分为5个一组的mini_batch
    mini_batches=[code_binary[k:k+5] for k in range(0,len(code_binary),5)]
    if len(mini_batches[-1])<5:#最后一个分组不足5个则补0
        mini_batches[-1]=mini_batches[-1]+(5-len(mini_batches[-1]))*[0]
    #对每个mini_batch计算十进制
    decimalism=[binary2decimalism(i) for i in mini_batches]
    #转换为base32编码
    code_base32=[base32[decimalism[i]] for i in range(len(decimalism))]
    str_base32=''.join(code_base32)
    return str_base32

#获取geohash
def get_geohash(longitude,latitude,precision):
    code_binary=get_binary(longitude,latitude,precision)#获取二进制编码
    geohash=get_base32(code_binary)#获取base32编码
    return geohash


# longitude=input('请输入经度:')
# latitude=input('请输入纬度:')
# precision=input('请输入精度:')
# print('geohash编码为:',get_geohash(float(longitude),float(latitude),int(precision)))