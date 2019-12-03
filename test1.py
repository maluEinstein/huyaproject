import os
days=os.listdir('C:/Users/hasee/Downloads/1')
for day in days:
    times = os.listdir('C:/Users/hasee/Downloads/1/'+day)
    for i in times:
        time=i.split('.txt')[0]
        path = 'C:/Users/hasee/Downloads/1/' + day + '/' + time + '.txt'
        print(path)
        with open(path, 'r', encoding="UTF-8") as fp:
            r = fp.readlines()
        r = map(lambda x: str(day + '&' + time + '&' + x), r)
        with open('C:/Users/hasee/Downloads/2/' + day + '.txt', 'a', encoding="UTF-8") as fw:
            fw.writelines(list(r))