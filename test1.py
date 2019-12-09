# import os
# days=os.listdir('C:/Users/hasee/Downloads/1')
# for day in days:
#     times = os.listdir('C:/Users/hasee/Downloads/1/'+day)
#     for i in times:
#         time=i.split('.txt')[0]
#         path = 'C:/Users/hasee/Downloads/1/' + day + '/' + time + '.txt'
#         print(path)
#         with open(path, 'r', encoding="UTF-8") as fp:
#             r = fp.readlines()
#         with open('C:/Users/hasee/Downloads/2/' + day + '.txt', 'a', encoding="UTF-8") as fw:
#             fw.writelines(list(r))
import pymysql
#SELECT * FROM `room_hot_analsis` where day="2019-11-14" and hour=23 ORDER BY `avg(room_hot)` LIMIT 100
day = "2019-11-14"
hour = "23"
conn = pymysql.connect(host='192.168.56.112', port=3306, user='hadoop', password= 'Hadoop@123', db='huya',
                       charset='utf8')
cursor = conn.cursor()
# sql="SELECT * FROM room_hot_analsis where day = "  + day +  "AND hour=" + hour + " ORDER BY 'avg(room_hot)' LIMIT 100"
sql = 'SELECT * FROM `room_hot_analsis` where day= "'+day+'" and hour='+hour+' ORDER BY `avg(room_hot)` LIMIT 100'
# and hour= ' + hour + '
res = cursor.execute(sql)
print(cursor.fetchmany(10))
conn.commit()
