from flask import Flask, request
from flask_cors import cross_origin

app = Flask(__name__)
result = {}


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/display', methods=['POST'])
@cross_origin()
def display():
    calData()
    return result


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
    res = []
    gameName = []
    gameHot = []
    gameRoom = []
    # 数据库处理
    # res=huyaMysql.selectData()
    # print(res)
    # 文件处理
    with open('newUpload.txt', encoding='utf-8') as fp:
        res = fp.readlines()
    # print(res)
    # for i in res:
    #     if i.split('&').__len__() <= 1:
    #         continue
    #     if i.split('&')[1] == '0':
    #         continue
    #     if float(i.split('&')[2].split('\n')[0]) < 3000000:
    #         continue
    #     gameName.append(i.split('&')[0])
    #     gameRoom.append(i.split('&')[1])

    #     gameHot.append(i.split('&')[2].split('万')[0])

    # 过滤热度少于100000的信息
    filter(lambda x: int(x.split('&')[-1].split('\n')[0]) > 100000, res)
    for i in res:
        sarray = i.split('&')
        if sarray.__len__() == 5:
            if gameName.__contains__(sarray[0]):
                gameRoom[gameName.index(sarray[0])] += 1
                gameHot[gameName.index(sarray[0])] += int(sarray[-1].split('\n')[0])
            else:
                gameName.append(sarray[0])
                gameRoom.append(1)
                gameHot.append(int(sarray[-1].split('\n')[0]))
    # 过滤房间数
    for i in gameRoom[::-1]:
        if int(i) < 50:
            index = gameRoom.index(i)
            print(index)
            gameRoom.remove(gameRoom[index])
            gameHot.remove(gameHot[index])
            gameName.remove(gameName[index])
    result["gameName"] = gameName
    result["gameRoom"] = gameRoom
    result['gameHot'] = gameHot


if __name__ == '__main__':
    app.run()
