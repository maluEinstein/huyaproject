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
    count = int(toSQL('SELECT COUNT(room_name) from tfidfdata WHERE game_name="' + game_name + '"')[0][0] / interval)
    for i in range(count):
        start = 1 + interval * i
        sql = 'SELECT room_name FROM `tfidfdata` WHERE game_name="' + game_name + '" order By day LIMIT ' + str(
            start) + ',' + str(
            interval)
        print(sql)
        longSentence = ''
        for sentence in toSQL(sql):
            longSentence = longSentence + str(sentence).split("'")[1]
        wordMsg1 = client.lexer(longSentence)
        wordMsg = wordMsg1['items']
        # 去除符号
        wordMsg = filter(lambda x: x['pos'] != 'w', wordMsg)
        finalres = ''
        for word in wordMsg:
            finalres = word['item'] + ' ' + finalres
        finalres = game_name + '&' + finalres + '\n'
        with open('E:/share/TFIDFData/' + game_name + '.txt', 'a', encoding="utf-8") as fw:
            fw.writelines(finalres)
        time.sleep(1)


# game_name = ['英雄联盟', '星秀', '王者荣耀', '交友', '一起看', '绝地求生', '和平精英', 'CF手游', 'lol云顶之弈', '魔兽世界', '我的世界', '穿越火线', '一起看']
# for i in game_name:
#     dealFile2(i)


def MLdata():
    sumData = 0
    right = 0
    # 计算正确率
    for label in range(5):
        for prediction in range(5):
            sql = 'SELECT COUNT(*) FROM `lrmlres` WHERE label=' + str(label) + ' AND prediction=' + str(prediction)
            num = int(toSQL(sql)[0][0])
            sumData = sumData + num
            if label == prediction:
                right = right + num
    print('正确率：' + str(right / sumData))
    print('样本总数：' + str(sumData))
    print('预测正确的数量：' + str(right))
    for label in range(6):
        sql = 'SELECT COUNT(*) FROM `lrmlres` WHERE label=' + str(label) + ' AND prediction !=' + str(label)
        sql1 = 'SELECT COUNT(*) FROM `lrmlres` WHERE label=' + str(label) + ' AND prediction =' + str(label)
        sql2 = 'SELECT COUNT(*) FROM `lrmlres` WHERE prediction=' + str(label) + ' AND label !=' + str(label)
        sql3 = 'SELECT COUNT(*) FROM `lrmlres` WHERE prediction!=' + str(label) + ' AND label !=' + str(label)
        num = int(toSQL(sql)[0][0])  # 将正类预测成负类
        num1 = int(toSQL(sql1)[0][0])  # 将正类预测成正类
        num2 = int(toSQL(sql2)[0][0])  # 负类预测成正类
        num3 = int(toSQL(sql3)[0][0])  # 负类预测成负类
        # 召回率
        if (num + num1) != 0:
            res = num1 / (num + num1)
            print('样本标签 ' + str(label) + '  的召回率：' + str(res))
        else:
            print('测试中没有该标签的数据')
        # 精确率
        if (num1 + num2) != 0:
            res1 = num1 / (num1 + num2)
            print('样本标签 ' + str(label) + '  的精确率：' + str(res1))
        else:
            print('测试中没有预测该标签的值')
dealFile1()
