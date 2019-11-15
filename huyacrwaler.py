import requests
from lxml import html
import threading, json
import time, os

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
                        res += gameName + '&' + gameRoom + '&' + RoomID + '&' + zhuoboname + '&' + str(gameHot) + '\n'
            # 不清楚要不要同步牺牲效率保证别出问题
            lock.acquire()
            day = str(self.wtime).split(' ')[0]
            hour = str(self.wtime).split(' ')[1]
            # path = 'd:/test/' + day + '/' + hour + '.txt'  # Windows环境设置生成的文件路径
            path = '/root/data/' + day + '/' + hour + '.txt'  # Linux环境设置生成的文件路径
            paths = path.split('/')
            dirpath = ''
            for i in paths:  # 去掉最后的xxx.txt
                if i.__contains__('.txt'):
                    continue
                else:
                    dirpath += i
                    dirpath += '/'
                if not os.path.isdir(dirpath):  # 判断文件夹是否已存在
                    os.mkdir(dirpath)
            with open(path, 'a', encoding="UTF-8") as fp:
                fp.writelines(res)
            # print(gid + 'write success by' + self.name)
            lock.release()


# 获取时间
# a='2000-1-1 00:00:00'
# timeinterval=10
# oldtime = time.mktime(time.strptime(a,'%Y-%m-%d %H:%M:%S'))
# newtime = time.strftime("%H", time.localtime())
# count=0
# while(True):
#     newtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
#     newtime = time.mktime(time.strptime(newtime, '%Y-%m-%d %H:%M:%S'))
#     if int(time.strftime("%M", time.localtime())) >21 or  int(time.strftime("%M", time.localtime())) < 2:
#         timeinterval=1
#     else:
#         timeinterval=10
#     if newtime-oldtime>timeinterval:
#         count+=1
#         print(str(timeinterval))
#         print('get one '+str(count))
#         oldtime=newtime
#     else:
#         continue
#

a = '2000-1-1 00:00:00'
timeInterval = 10  # 时间间隔
oldTime = time.strptime(a, '%Y-%m-%d %H:%M:%S')
while (True):
    if int(time.strftime("%H", time.localtime())) > 16 or int(time.strftime("%H", time.localtime())) < 2:
        # 16点到23点 0点到2点高峰段一小时爬一次 正常时间3小时爬一次
        timeInterval = 1
    else:
        timeInterval = 3
    if abs(int(time.strftime("%H", time.localtime())) - int(time.strftime("%H", oldTime))) >= timeInterval:
        # 通过绝对值来处理时间到第二天的时候oldTime的H会大于localtime的H
        oldTime = time.localtime()
        gidlist = []
        r = requests.get('https://www.huya.com/g', headers=headers)
        selector = html.etree.HTML(r.text)
        l = selector.xpath('//div[@class="box-bd"]/ul/li/a/@report')
        for i in range(len(l)):
            tmp = str(l[i])
            # 使用eval转化str为字典格式进行爬取
            tmp = eval(tmp)
            gidlist.append(tmp["game_id"])
        print(gidlist)
        print('开始时间：' + time.strftime('%Y-%m-%d %H:%M:%S', oldTime))
        wTime = time.strftime('%Y-%m-%d %H', time.localtime())
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
    else:
        continue
