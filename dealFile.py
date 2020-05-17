import os
import time

from aip import AipNlp
import pymysql


# 爬虫文件处理（小文件转成大文件）
def dealFile1():
    days = os.listdir('E:/tmp1/')
    for day in days:
        times = os.listdir('E:/tmp1/' + day)
        for i in times:
            time = i.split('.txt')[0]
            path = 'E:/tmp1/' + day + '/' + time + '.txt'
            print(path)
            with open(path, 'r', encoding="UTF-8") as fp:
                r = fp.readlines()
            # r = map(lambda x: str(day + '&' + time + '&' + x), r)
            with open('E:/share/data/' + day + '.txt', 'a', encoding="UTF-8") as fw:
                fw.writelines(list(r))


# TF-IDF文件生成器
def toSQL(sql):
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='hadoop', password='Hadoop@123', db='huya',
                           charset='utf8')
    cursor = conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()


def dealFile2(game_name):
    APP_ID = '18685069'
    API_KEY = 'xhX16HGosqbT6MGfcKKAAiAj'
    SECRET_KEY = 'ZvPq3IKfSzT8crw1dL2Eya9UV4qVFqlK'
    client = AipNlp(APP_ID, API_KEY, SECRET_KEY)
    interval = 200
    count = int(toSQL('SELECT COUNT(room_name) from tfidfdata ')[0][0])
    # 稍微提高下程序运行速度
    if count >= 200:
        count = 200
    for i in range(count):
        start = 1 + interval * i
        sql = 'SELECT room_name FROM `tfidfdata` WHERE game_name="' + game_name + '"LIMIT ' + str(start) + ',' + str(
            interval)
        print(sql)
        longSentence = ''
        for sentence in toSQL(sql):
            longSentence = longSentence + str(sentence).split("'")[1]
        wordMsg = client.lexer(longSentence)['items']
        # 去除符号
        wordMsg = filter(lambda x: x['pos'] != 'w', wordMsg)
        finalres = ''
        for word in wordMsg:
            finalres = finalres + ' ' + word['item']
        finalres = finalres + '\n'
        with open('E:/share/TFIDFData/' + game_name + '.txt', 'a') as fw:
            fw.writelines(finalres)
        time.sleep(1)


game_name = ['星秀', '王者荣耀', '交友', '一起看', '绝地求生', '和平精英', 'CF手游', 'lol云顶之弈', '魔兽世界', '我的世界', '穿越火线', '一起看']
for i in game_name:
    dealFile2(i)
