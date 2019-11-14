import requests
from lxml import html
import os,json

gidlist = []
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 '
                         '(KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36'}
r = requests.get('https://www.huya.com/g', headers=headers)
selector = html.etree.HTML(r.text)
l = selector.xpath('//div[@class="box-bd"]/ul/li/a/@report')
# print(l)
for i in range(len(l)):
    tmp=str(l[i])
    tmp=eval(tmp)
    gidlist.append(tmp["game_id"])
print(gidlist)
path = 'e:/root/1.txt'
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
    fp.writelines(str(gidlist))
