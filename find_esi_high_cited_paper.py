from MAG_paper.tools import connect_to_table, timestamp

def find_hcp():
    col1 = connect_to_table('wangwenbin', 'MAG_Physics_Cite')

    for i in col1.find({'year': {'$lt': 2008}}).batch_size(1000):
        a = i['year']




if __name__ == '__main__':
    start = timestamp()
    find_hcp()
    end = timestamp()
    print('用时'+str(end-start))