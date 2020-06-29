from flask import Flask, request, render_template
from flask_cors import cross_origin
import pymysql

app = Flask(__name__)
result = {}


@app.route('/')
def hello_world():
    return render_template('upload.html')  # send_file('index.html')


@app.route('/displayPYData', methods=['POST'])
@cross_origin()
def display():
    result.clear()
    calData()
    return result


# 查询时间段内直播指标变化
@app.route('/TypeChangeByDay', methods=['POST'])
@cross_origin()
def TypeChangeByDay():
    result.clear()
    days = ['days']
    start = str(request.data).split("'")[1].split('&')[0].split('=')[1]
    end = str(request.data).split("'")[1].split('&')[1].split('=')[1]
    index = 0
    res = []
    game_name = ['英雄联盟', '星秀', '王者荣耀', '交友', '一起看', '绝地求生', '和平精英', 'CF手游', 'lol云顶之弈', '魔兽世界', '我的世界', '穿越火线', '一起看']
    sql = 'SELECT day,game_name,SUM(`sum(room_hot)`) from (SELECT * FROM room_hot_analsis WHERE (game_name= "' + \
          game_name[0] + '"'
    for i in range(1, game_name.__len__()):
        sql = sql + ' or '
        sql = sql + 'game_name = "' + game_name[i] + '"'
    sql = sql + ') and day BETWEEN  "' + start + '" and "' + end + '" ) as tmp GROUP BY day,game_name '
    print(sql)
    # 保存数据库搜索出的结果
    SQLres = toSQL(sql)
    # 设置游戏分类的列表再填入数据
    for i in SQLres:
        res.append([str(i[1])])
        index = index + 1
        if index == game_name.__len__() - 1:
            index = 0
            break
    for i in SQLres:
        res[index].append(str(i[2]))
        index = index + 1
        if index == game_name.__len__() - 1:
            days.append(str(i[0]))
            index = 0
    res.insert(0, days)
    print(res)
    result['dataSet'] = res
    return result


# 查询单日直播分类指标
@app.route('/displayDataByDay', methods=['POST'])
@cross_origin()
def displayDataByDay():
    result.clear()
    gameName = []
    dataMax = []
    dataAvg = []
    dataSum = []
    dataCount = []
    day = str(request.data).split("'")[1].split('&')[0].split('=')[1]
    sql = 'SELECT game_name,MAX(`max(room_hot)`),ROUND(AVG(`avg(room_hot)`)),SUM(`sum(room_hot)`), SUM(`count(room_hot)`) FROM' \
          ' (SELECT * FROM room_hot_analsis WHERE day="' + day + '" and `count(room_hot)` >250) as tmp GROUP BY game_name ORDER BY SUM(`count(room_hot)`) desc'
    for i in toSQL(sql):
        gameName.append(str(i[0]))
        dataMax.append(str(i[1]))
        dataAvg.append(str(i[2]))
        dataSum.append(str(i[3]))
        dataCount.append(str(i[4]))
    result["gameName"] = gameName
    result['dataMax'] = dataMax
    result['dataAvg'] = dataAvg
    result['dataSum'] = dataSum
    result['dataCount'] = dataCount
    return result


@app.route('/displayDataByType', methods=['POST'])
@cross_origin()
def displayDatabytype():
    result.clear()
    days = []
    dataMax = []
    dataAvg = []
    dataSum = []
    dataCount = []
    print(request.data)
    print(request.data.decode("utf-8"))
    game_name = str(request.data.decode("utf-8")).split('&')[0].split('=')[1]
    start = str(request.data).split("'")[1].split('&')[1].split('=')[1]
    end = str(request.data).split("'")[1].split('&')[2].split('=')[1]
    sql = 'SELECT day,MAX(`max(room_hot)`),ROUND(AVG(`avg(room_hot)`)),SUM(`sum(room_hot)`), SUM(`count(room_hot)`) FROM' \
          '(SELECT * FROM `room_hot_analsis` where game_name="' + game_name + '" and day BETWEEN  "' + start + '"and "' + end + '")as tmp GROUP BY day ORDER BY day '
    for i in toSQL(sql):
        days.append(str(i[0]))
        dataMax.append(str(i[1]))
        dataAvg.append(str(i[2]))
        dataSum.append(str(i[3]))
        dataCount.append(str(i[4]))
    result["days"] = days
    result['dataMax'] = dataMax
    result['dataAvg'] = dataAvg
    result['dataSum'] = dataSum
    result['dataCount'] = dataCount
    return result


@app.route('/MLdata', methods=['POST'])
@cross_origin()
def MLdata():
    result.clear()
    sumData = 0
    right = 0
    rightMsg = ''
    # 获取参数
    tableName = ''
    requestData = str(request.data)
    if len(requestData.split("&")) >= 2:
        if len(requestData.split("&")) == 2:
            tableName = 'rfmlres'
            tableName = tableName + requestData.split("'")[1].split('&')[0].split('=')[1] + \
                        requestData.split("'")[1].split('&')[1].split('=')[1]
        else:
            tableName = requestData.split("'")[1].split('&')[0].split('=')[1]
    else:
        tableName = 'lrmlres'
        tableName = tableName + requestData.split("'")[1].split('&')[0].split('=')[1]
    # 计算正确率
    for label in range(5):
        for prediction in range(5):
            sql = 'SELECT COUNT(*) FROM `' + tableName + '` WHERE label=' + str(label) + ' AND prediction=' + str(
                prediction)
            num = int(toSQL(sql)[0][0])
            sumData = sumData + num
            if label == prediction:
                right = right + num
    rightMsg = rightMsg + '正确率：' + str(right / sumData) + '\n'
    rightMsg = rightMsg + '样本总数：' + str(sumData) + '\n'
    rightMsg = rightMsg + '预测正确的数量：' + str(right) + '\n'
    print('正确率：' + str(right / sumData))
    print('样本总数：' + str(sumData))
    print('预测正确的数量：' + str(right))
    acc = []
    recall = []
    labelList = [0, 1, 2, 3, 4, 5]
    for label in range(6):
        sql = 'SELECT COUNT(*) FROM `' + tableName + '` WHERE label=' + str(label) + ' AND prediction !=' + str(label)
        sql1 = 'SELECT COUNT(*) FROM `' + tableName + '` WHERE label=' + str(label) + ' AND prediction =' + str(label)
        sql2 = 'SELECT COUNT(*) FROM `' + tableName + '` WHERE prediction=' + str(label) + ' AND label !=' + str(label)
        sql3 = 'SELECT COUNT(*) FROM `' + tableName + '` WHERE prediction!=' + str(label) + ' AND label !=' + str(label)
        num = int(toSQL(sql)[0][0])  # 将正类预测成负类
        num1 = int(toSQL(sql1)[0][0])  # 将正类预测成正类
        num2 = int(toSQL(sql2)[0][0])  # 负类预测成正类
        num3 = int(toSQL(sql3)[0][0])  # 负类预测成负类
        # 召回率
        if (num + num1) != 0:
            res = num1 / (num + num1)
            acc.append(res)
            print('标签' + str(label) + '召回率为' + str(res))
        else:
            recall.append(0)
            print('标签' + str(label) + '在测试中没有该标签的数据')
        # 精确率
        if (num1 + num2) != 0:
            res1 = num1 / (num1 + num2)
            recall.append(res1)
            print('标签' + str(label) + '精确率为' + str(res1))
        else:
            recall.append(0)
            print('标签' + str(label) + '在测试中没有预测该标签的值')
    rightMsg = rightMsg + '0：无热度  1：高热度  2：顶级热度 3：一般热度 4：低热度  5：超高热度' + '\n'
    rightMsg = rightMsg + '红色：召回 藏青色：精确率' + '\n'
    result['rightMsg'] = rightMsg
    result['acc'] = acc
    result['recall'] = recall
    result['labelList'] = labelList
    return result


# 历史代码
@app.route('/test', methods=['POST'])
@cross_origin()
def test():
    result.clear()
    LabelList = []
    numList = []
    chance = {0: '无热度', 1: '高热度', 2: '顶级热度', 3: '一般热度', 4: '低热度', 5: '超高热度'}
    for i in range(5):
        sql = 'SELECT count(*) FROM `lrmlres2000` WHERE label=' + str(i)
        num = int(toSQL(sql)[0][0])
        LabelList.append(chance.get(i))
        numList.append(num)
    result['gameName'] = LabelList
    result['dataCount'] = numList
    return result


@app.route('/displayDatabyroom', methods=['POST'])
@cross_origin()
def displayDatabyroom():
    result.clear()
    gameHot = []
    gametime = []
    print(str(request.data))
    room_id = str(request.data).split("'")[1].split('=')[1]
    sql = 'SELECT * FROM `basedata` WHERE room_id=' + room_id + ' ORDER BY day,hour'
    # gametime=['2019-12-10 {}'.format(str(i))for i in range(23)].extend(['2019-12-11 {}'.format(str(i))for i in range(23)])
    # for i in gametime:
    #     print(gametime)
    for i in set(toSQL(sql)):
        gameHot.append(str(i[6]))
        gametime.append(str(i[0]) + " " + str(i[1]))
    result["gameHot"] = gameHot
    result["gametime"] = gametime
    return result


@app.route('/upload', methods=['POST'])
@cross_origin()
def upload():
    f = request.files['file']
    f.save('newUpload.txt')
    return 'file uploaded success'


def calData():
    result.clear()
    gameName = []
    gameHot = []
    gameRoom = []
    gameAve = []
    # 文件处理
    with open('newUpload.txt', encoding='utf-8') as fp:
        res = fp.readlines()

    # 过滤主播信息中带有分隔符的信息（后期考虑数据清洗）
    res = filter(lambda x: len(x.split('&')) == 7, res)
    # 过滤热度少于100000的信息
    res = filter(lambda x: int(x.split('&')[-1].split('\n')[0]) > 100000, res)
    # map方法得到游戏分类列表
    # gameName = list(set(map(lambda x: x.split('&')[2], res)))
    # gameRoom = list(map(lambda x: (x.split('&')[-1].split('\n')[0], 1), res))

    for i in res:
        sarray = i.split('&')
        if gameName.__contains__(sarray[2]):
            gameRoom[gameName.index(sarray[2])] += 1
            gameHot[gameName.index(sarray[2])] += int(sarray[-1].split('\n')[0])
        else:
            gameName.append(sarray[2])
            gameRoom.append(1)
            gameHot.append(int(sarray[-1].split('\n')[0]))
            gameAve.append(0)

    # 过滤房间数

    for i in gameRoom[::-1]:
        if int(i) < 50 * 7:
            index = gameRoom.index(i)
            gameRoom.remove(gameRoom[index])
            gameHot.remove(gameHot[index])
            gameName.remove(gameName[index])
            gameAve.remove(gameAve[index])
        else:
            index = gameRoom.index(i)
            gameAve[index] = gameHot[index] / gameRoom[index]
    result["gameName"] = gameName
    result["gameAve"] = gameAve
    result['gameHot'] = gameHot
    result['gameRoom'] = gameRoom


def toSQL(sql):
    # 本地运行时访问的数据库
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='hadoop', password='Hadoop@123', db='huya',
                           charset='utf8')
    # 阿里云上运行访问的数据库
    # conn = pymysql.connect(host='localhost', port=3306, user='hadoop', password='Hadoop@123', db='huya',
    #                        charset='utf8')
    cursor = conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()


if __name__ == '__main__':
    app.run()
