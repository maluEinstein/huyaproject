from flask import Flask, request, render_template
from flask_cors import cross_origin
from functools import reduce

app = Flask(__name__)
result = {}


@app.route('/')
def hello_world():
    return render_template('upload.html')  # send_file('index.html')


@app.route('/display', methods=['POST'])
@cross_origin()
def display():
    calData()
    return result


@app.route('/display1', methods=['POST'])
@cross_origin()
def display1():
    pass


@app.route('/upload', methods=['POST'])
@cross_origin()
def upload():
    f = request.files['file']
    f.save('newUpload.txt')
    return 'file uploaded success'


# @app.route('/insertToDB', methods=['POST'])
# @cross_origin()
# def insertToDB():
#     return 'insert success'


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


if __name__ == '__main__':
    app.run()
