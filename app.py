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
    days = []
    daySum = []
    start = str(request.data).split("'")[1].split('&')[0].split('=')[1]
    end = str(request.data).split("'")[1].split('&')[1].split('=')[1]
    game_name = ['英雄联盟', '星秀', '王者荣耀', '交友', '一起看', '绝地求生', '和平精英', 'CF手游', 'lol云顶之弈', '魔兽世界', '我的世界', '穿越火线', '一起看']
    sql = 'SELECT day,game_name,SUM(`sum(room_hot)`) from (SELECT * FROM room_hot_analsis WHERE (game_name= " ' + \
          game_name[0] + '"'
    for i in range(1, game_name.__len__()):
        sql = sql + ' or '
        sql = sql + 'game_name = "' + game_name[i] + '"'
    sql = sql + ') and day BETWEEN  "' + start + '" and "' + end + '" ) as tmp GROUP BY day,game_name '
    print(sql)
    for i in toSQL(sql):
        days.append(str(i[0]))
        daySum.append(str(i[2]))
    result['days'] = days
    result['']
    result['daySum'] = daySum
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
          ' (SELECT * FROM room_hot_analsis WHERE day="' + day + '" and `count(room_hot)` >100) as tmp GROUP BY game_name ORDER BY SUM(`count(room_hot)`) desc'

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


@app.route('/displayMysqlDatabyroom', methods=['POST'])
@cross_origin()
def displayMysqlDatabyroom():
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
