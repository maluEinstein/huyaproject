# 单线程脚本爬取数据测试
# import requests
# from lxml import html
# import threading, json
# import time, os
#
# headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 '
#                          '(KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36'}
#
# lock = threading.Lock()
#
#
# class crawlerThread(threading.Thread):
#     def __init__(self, name, wtime):
#         threading.Thread.__init__(self)
#         self.name = name
#         self.wtime = wtime
#
#     def run(self):
#         while (True):
#             gameHot = 0
#             gameRoom = ''
#             gameName = ''
#             zhuoboname = ''
#             RoomID = ''
#             res = ''
#             lock.acquire()
#             if len(gidlist) <= 0:
#                 lock.release()
#                 break
#             gid = gidlist[0]
#             gidlist.remove(gid)
#             lock.release()
#             urls = ['https://www.huya.com/cache.php?m=LiveList&do=getLiveListByPage&gameId=' + str(
#                 gid) + '&tagAll=0&page={}'.format(str(i)) for i in range(1, 30)]
#             for url in urls:
#                 # print(self.name + url)
#                 r = requests.get(url, headers=headers)
#                 if r.status_code == 200:
#                     msg = json.loads(r.text)
#                     for j in msg['data']['datas']:
#                         # 处理一个page信息里包含多个模块的直播
#                         # 处理方式：不将这页并入计算（往往这类直播都在另外的板块还有出现）
#                         gameHot = int(j['totalCount'])
#                         if gameHot == 0:  # 排除无效信息
#                             continue
#                         zhuoboname = str(j['nick'])
#                         gameName = str(j['gameFullName'])
#                         gameRoom = str(j['roomName'])
#                         RoomID = str(j['profileRoom'])
#                         day = str(self.wtime).split(' ')[0]
#                         hour = str(self.wtime).split(' ')[1]
#                         res += day + '&' + hour + '&' + gameName + '&' + url.split("page=")[
#                             1] + '&' + gameRoom + '&' + RoomID + '&' + zhuoboname + '&' + str(
#                             gameHot) + '\n'
#             # 不清楚要不要同步牺牲效率保证别出问题
#             lock.acquire()
#             day = str(self.wtime).split(' ')[0]
#             hour = str(self.wtime).split(' ')[1]
#             path = 'e:/StreamData/' + day + '_' + hour + '.txt'  # Windows环境设置生成的文件路径
#             # path = '/root/data/' + day + '/' + hour + '.txt'  # Linux环境设置生成的文件路径
#             paths = path.split('/')
#             dirpath = ''
#             for i in paths:  # 去掉最后的xxx.txt
#                 if i.__contains__('.txt'):
#                     continue
#                 else:
#                     dirpath += i
#                     dirpath += '/'
#                 if not os.path.isdir(dirpath):  # 判断文件夹是否已存在
#                     os.mkdir(dirpath)
#             with open(path, 'a', encoding="UTF-8") as fp:
#                 fp.writelines(res)
#             # print(gid + 'write success by' + self.name)
#             lock.release()
#
#
#
# a = '2000-1-1 00:00:00'
# timeInterval = 1  # 时间间隔
# oldTime = time.strptime(a, '%Y-%m-%d %H:%M:%S')
# while (True):
#     if abs(int(time.strftime("%H", time.localtime())) - int(time.strftime("%H", oldTime))) >= timeInterval:
#         # 通过绝对值来处理时间到第二天的时候oldTime的H会大于localtime的H
#         oldTime = time.localtime()
#         gidlist = []
#         r = requests.get('https://www.huya.com/g', headers=headers)
#         selector = html.etree.HTML(r.text)
#         l = selector.xpath('//div[@class="box-bd"]/ul/li/a/@data-gid')
#         for i in l:
#             gidlist.append(i)
#         print(gidlist)
#         print('开始时间：' + time.strftime('%Y-%m-%d %H:%M:%S', oldTime))
#         wTime = time.strftime('%Y-%m-%d %H', time.localtime())
#         t1 = crawlerThread('A', wTime)
#         t1.start()
#         t1.join()
#         print('结束时间' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
#     else:
#         time.sleep(1000)


# 写文件爬取测试
import time
path = 'E:\\StreamData\\test.txt'
while True:
    time.sleep(1)
    with open(path, 'a', encoding='utf-8') as fp:
        fp.writelines(str(time.time()) + '\n')
