"""
1179s
"""
from MAG_paper.tools import connect_to_table, timestamp
import multiprocessing


def get_first_year(col2, author_id):
    year = []
    for i in col2.find({'authors.id': author_id}):
        pub_year = i.get('year')
        if pub_year:
            year.append(pub_year)
            break
    if len(year) == 0:
        return 0
    else:
        return min(year)


def count_author_age(a, b, ids, pro_id):
    col1 = connect_to_table('wangwenbin', 'MAG_Physics_Cite')
    col2 = connect_to_table('academic', 'mag_papers')
    col3 = connect_to_table('wangwenbin', 'author_relative')

    pro_id = pro_id
    count = 0
    yijing = 0
    fail = 0
    success = 0
    papers = []

    for i in col1.find()[a:b].batch_size(100):
        count += 1
        authors = i.get('new_authors')
        pub_year = i.get('year')
        new_authors = []
        if authors and pub_year:
            for author in authors:
                author_info = {}
                author_id = author.get('id')
                author_name = author.get('name')
                already = ids.get(author_id)
                if not already:
                    first_year = get_first_year(col2, author_id)
                    ids[author_id] = first_year
                    age = pub_year - first_year
                    author_info['author_age'] = age
                    author_info['author_id'] = author_id
                    author_info['author_name'] = author_name
                else:
                    yijing += 1
                    age = pub_year - already
                    author_info['author_age'] = age
                    author_info['author_id'] = author_id
                    author_info['author_name'] = author_name
                new_authors.append(author_info)
            i['authors'] = new_authors
            papers.append(i)
            if len(papers) == 10000:
                col3.insert_many(papers)
                papers.clear()
                success += 1
                print(
                    '进程' + str(pro_id) + ', 累计查找' + str(count) + '，写入 ' + str(success * 10000) + ' 条, 已经' + str(yijing))
        else:
            fail += 1

    col3.insert_many(papers)
    print('进程' + str(pro_id) + ', 累计查找 ' + str(count) + '，写入 ' + str(success * 10000 + len(papers)) + ' 条，已经' + str(
        yijing))
    print('无authors或者year的有 ' + str(fail))


if __name__ == '__main__':
    a = timestamp()
    ids = multiprocessing.Manager().dict()

    p1 = multiprocessing.Process(target=count_author_age, args=(240000 * 0, 240000 * 0 + 240000, ids, 1))
    p2 = multiprocessing.Process(target=count_author_age, args=(240000 * 1, 240000 * 1 + 240000, ids, 2))
    p3 = multiprocessing.Process(target=count_author_age, args=(240000 * 2, 240000 * 2 + 240000, ids, 3))
    p4 = multiprocessing.Process(target=count_author_age, args=(240000 * 3, 240000 * 3 + 240000, ids, 4))
    p5 = multiprocessing.Process(target=count_author_age, args=(240000 * 4, 240000 * 4 + 240000, ids, 5))
    p6 = multiprocessing.Process(target=count_author_age, args=(240000 * 5, 240000 * 5 + 240000, ids, 6))
    p7 = multiprocessing.Process(target=count_author_age, args=(240000 * 6, 240000 * 6 + 240000, ids, 7))
    p8 = multiprocessing.Process(target=count_author_age, args=(240000 * 7, 240000 * 7 + 240000, ids, 8))
    p9 = multiprocessing.Process(target=count_author_age, args=(240000 * 8, 240000 * 8 + 240000, ids, 9))
    p10 = multiprocessing.Process(target=count_author_age, args=(240000 * 9, 240000 * 9 + 240000, ids, 10))

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