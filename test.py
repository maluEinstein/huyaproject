# 测试爬虫数据
# import requests
# from lxml import html
# import os, json
#
# gidlist = []
# headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 '
#                          '(KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36'}
# r = requests.get('https://www.huya.com/g', headers=headers)
# selector = html.etree.HTML(r.text)
# l = selector.xpath('//div[@class="box-bd"]/ul/li/a/@data-gid')
# for i in l:
#     gidlist.append(i)
# print(gidlist)
# path = 'e:/root/1.txt'
# paths = path.split('/')
# dirpath = ''
# for i in paths:  # 去掉最后的xxx.txt
#     if i.__contains__('.txt'):
#         continue
#     else:
#         dirpath += i
#         dirpath += '/'
#     if not os.path.isdir(dirpath):  # 判断文件夹是否已存在
#         os.mkdir(dirpath)
# with open(path, 'a', encoding="UTF-8") as fp:
#     fp.writelines(str(gidlist))

# 数据库测试
# import pymysql
# #SELECT * FROM `room_hot_analsis` where day="2019-11-14" and hour=23 ORDER BY `avg(room_hot)` LIMIT 100
# day = "2019-11-14"
# hour = "23"
# conn = pymysql.connect(host='192.168.56.112', port=3306, user='hadoop', password= 'Hadoop@123', db='huya',
#                        charset='utf8')
# cursor = conn.cursor()
# # sql="SELECT * FROM room_hot_analsis where day = "  + day +  "AND hour=" + hour + " ORDER BY 'avg(room_hot)' LIMIT 100"
# sql = 'SELECT * FROM `room_hot_analsis` where day= "'+day+'" and hour='+hour+' ORDER BY `avg(room_hot)` LIMIT 100'
# # and hour= ' + hour + '
# res = cursor.execute(sql)
# print(cursor.fetchmany(10))
# conn.commit()

# urls = ['https://www.huya.com/cache.php?m=LiveList&do=getLiveListByPage&gameId=' + '&tagAll=0&page={}'.format(str(i))
#         for i in range(1, 30)]
# for url in urls:
#     print(url.split("page=")[1])


import json
import threading
import time
import requests
# KafkaTest生产者测试
from kafka import KafkaProducer
from lxml import html

# 实例化一个KafkaProducer示例，用于向Kafka投递消息
producer = KafkaProducer(bootstrap_servers='localhost:9092')
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 '
                         '(KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36'}
lock = threading.Lock()


class crawlerThread(threading.Thread):
    def __init__(self, name, wtime):
        threading.Thread.__init__(self)
        self.name = name
        self.wtime = wtime

    def run(self):
        while (True):
            gameHot = 0
            gameRoom = ''
            gameName = ''
            zhuoboname = ''
            RoomID = ''
            res = ''
            lock.acquire()
            if len(gidlist) <= 0:
                lock.release()
                break
            gid = gidlist[0]
            gidlist.remove(gid)
            lock.release()
            urls = ['https://www.huya.com/cache.php?m=LiveList&do=getLiveListByPage&gameId=' + str(
                gid) + '&tagAll=0&page={}'.format(str(i)) for i in range(1, 30)]
            for url in urls:
                # print(self.name + url)
                r = requests.get(url, headers=headers)
                if r.status_code == 200:
                    msg = json.loads(r.text)
                    for j in msg['data']['datas']:
                        # 处理一个page信息里包含多个模块的直播
                        # 处理方式：不将这页并入计算（往往这类直播都在另外的板块还有出现）
                        gameHot = int(j['totalCount'])
                        if gameHot == 0:  # 排除无效信息
                            continue
                        zhuoboname = str(j['nick'])
                        gameName = str(j['gameFullName'])
                        gameRoom = str(j['roomName'])
                        RoomID = str(j['profileRoom'])
                        day = str(self.wtime).split(' ')[0]
                        hour = str(self.wtime).split(' ')[1]
                        topic=day.split('-')[0]+day.split('-')[1]+day.split('-')[2]+hour
                        res = day + '&' + hour + '&' + gameName + '&' + url.split("page=")[
                            1] + '&' + gameRoom + '&' + RoomID + '&' + zhuoboname + '&' + str(
                            gameHot) + '\n'
                        producer.send(topic, value=res.encode('utf-8'))


while (True):
    gidlist = []
    r = requests.get('https://www.huya.com/g', headers=headers)
    selector = html.etree.HTML(r.text)
    l = selector.xpath('//div[@class="box-bd"]/ul/li/a/@data-gid')
    for i in l:
        gidlist.append(i)
    print(gidlist)
    oldTime = time.localtime()
    print('开始时间：' + time.strftime('%Y-%m-%d %H:%M:%S', oldTime))
    wTime = time.strftime('%Y-%m-%d %H', oldTime)
    t1 = crawlerThread('A', wTime)
    t2 = crawlerThread('B', wTime)
    t3 = crawlerThread('C', wTime)
    t4 = crawlerThread('D', wTime)
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    print('结束时间' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
