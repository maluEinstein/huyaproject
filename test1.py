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