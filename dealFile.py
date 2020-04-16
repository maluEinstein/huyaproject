import os
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
