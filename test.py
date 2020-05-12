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




























