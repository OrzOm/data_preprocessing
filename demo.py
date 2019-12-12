with open('./data_preprocessing/cou.txt', 'r') as f:
    for i in f.readlines():
        data = i.split(',')
        print(data)