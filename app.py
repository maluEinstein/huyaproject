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


@app.route('/insertToDB', methods=['POST'])
@cross_origin()
def insertToDB():
    return 'insert success'


def calData():
    res = []
    gameName = []
    gameHot = []
    gameRoom = []
    #数据库处理
    # res=huyaMysql.selectData()
    # print(res)
    # 文件处理
    with open('newUpload.txt', encoding='gbk') as fp:
        res = fp.readlines()
    print(res)
    for i in res:
        if i.split('&').__len__() <= 1:
            continue
        if i.split('&')[1] == '0':
            continue
        if float(i.split('&')[2].split('\n')[0]) < 3000000:
            continue
        gameName.append(i.split('&')[0])
        gameRoom.append(i.split('&')[1])
        gameHot.append(i.split('&')[2].split('万')[0])
    result["gameName"] = gameName
    result["gameRoom"] = gameRoom
    result['gameHot'] = gameHot


if __name__ == '__main__':
    app.run()
