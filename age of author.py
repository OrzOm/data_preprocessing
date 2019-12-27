"""
2累计查找 278000，写入 278000
1累计查找 278000，写入 278000
8累计查找 278000，写入 278000
6累计查找 278000，写入 278000
7累计查找 278000，写入 278000
10累计查找 274429，写入 274429
5累计查找 278000，写入 278000
4累计查找 278000，写入 278000
3累计查找 278000，写入 278000
9累计查找 278000，写入 278000
4547.753385782242
"""
from MAG_paper.tools import connect_to_table, timestamp
import multiprocessing


def get_first_year(col2, author_id):
    year = 2019
    for i in col2.find({'authors.id': author_id}):
        pub_year = i.get('year')
        if pub_year and pub_year < year:
            year = pub_year
    return year


def count_author_age(a, b, proc_id):
    col1 = connect_to_table('wangwenbin', 'phy_author_table')
    col2 = connect_to_table('academic', 'mag_papers')
    col3 = connect_to_table('wangwenbin', 'phy_author_table_2')
    count = 0
    papers = []

    for i in col1.find()[a:b].batch_size(100):
        count += 1
        author_info = {}
        author_id = i.get('_id')
        first_year = get_first_year(col2, author_id)
        author_info["_id"] = author_id
        author_info['first_year'] = first_year
        papers.append(author_info)
        if count % 10000 == 0:
            print(count)
    col3.insert_many(papers)
    print(str(proc_id)+'累计查找 ' + str(count) + '，写入 ' + str(len(papers)))


if __name__ == '__main__':
    a = timestamp()
    p1 = multiprocessing.Process(target=count_author_age, args=(278000 * 0, 278000 * 0 + 278000, 1))
    p2 = multiprocessing.Process(target=count_author_age, args=(278000 * 1, 278000 * 1 + 278000, 2))
    p3 = multiprocessing.Process(target=count_author_age, args=(278000 * 2, 278000 * 2 + 278000, 3))
    p4 = multiprocessing.Process(target=count_author_age, args=(278000 * 3, 278000 * 3 + 278000, 4))
    p5 = multiprocessing.Process(target=count_author_age, args=(278000 * 4, 278000 * 4 + 278000, 5))
    p6 = multiprocessing.Process(target=count_author_age, args=(278000 * 5, 278000 * 5 + 278000, 6))
    p7 = multiprocessing.Process(target=count_author_age, args=(278000 * 6, 278000 * 6 + 278000, 7))
    p8 = multiprocessing.Process(target=count_author_age, args=(278000 * 7, 278000 * 7 + 278000, 8))
    p9 = multiprocessing.Process(target=count_author_age, args=(278000 * 8, 278000 * 8 + 278000, 9))
    p10 = multiprocessing.Process(target=count_author_age, args=(278000 * 9, 278000 * 9 + 278000, 10))

    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p5.start()
    p6.start()
    p7.start()
    p8.start()
    p9.start()
    p10.start()

    p1.join()
    p2.join()
    p3.join()
    p4.join()
    p5.join()
    p6.join()
    p7.join()
    p8.join()
    p9.join()
    p10.join()

    b = timestamp()
    print(b - a)