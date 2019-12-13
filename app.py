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


# 查询mysql里的数据
@app.route('/displayMysqlDatabytime', methods=['POST'])
@cross_origin()
def displayMysqlDatabytime():
    result.clear()
    gameName = []
    gameHot = []
    day = str(request.data).split("'")[1].split('&')[0].split('=')[1]
    hour = str(request.data).split("'")[1].split('&')[1].split('=')[1]
    sql = 'SELECT * FROM `room_hot_analsis` where day= "' + day + '" and hour=' + hour + ' ORDER BY `avg(room_hot)` LIMIT 100'
    for i in toSQL(sql):
        gameName.append(str(i[2]))
        gameHot.append(str(i[3]))
    result["gameName"] = gameName
    result['gameHot'] = gameHot
    print(result)
    return result


@app.route('/displayMysqlDatabytype', methods=['POST'])
@cross_origin()
def displayMysqlDatabytype():
    result.clear()
    gameHot = []
    gametime = []
    game_name = str(request.data).split("'")[1].split('=')[1]
    print(game_name)
    game_name = "梦三国"
    sql = 'SELECT * FROM `room_hot_analsis` where game_name="' + game_name + '" ORDER BY day,hour '
    for i in toSQL(sql):
        print(i[3])
        gameHot.append(str(i[3]))
        gametime.append(str(i[0]) + " " + str(i[1]))
    result["gameHot"] = gameHot
    result['gametime'] = gametime
    return result


@app.route('/displayMysqlDatabyroom', methods=['POST'])
@cross_origin()
def displayMysqlData2():
    result.clear()
    gameHot = []
    gametime = []
    print(str(request.data))
    room_id = str(request.data).split("'")[1].split('=')[1]
    sql = 'SELECT * FROM `basedata` WHERE room_id=' + room_id + ' ORDER BY day,hour'
    for i in toSQL(sql):
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
    conn = pymysql.connect(host='192.168.56.112', port=3306, user='hadoop', password='Hadoop@123', db='huya',
                           charset='utf8')
    cursor = conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()


if __name__ == '__main__':
    app.run()
