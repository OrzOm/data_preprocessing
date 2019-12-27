"""
8累计查找231000累计插入193274
3累计查找231000累计插入210005
9累计查找231000累计插入218404
7累计查找231000累计插入198863
6累计查找231000累计插入212452
5累计查找231000累计插入209411
1累计查找231000累计插入231000
10累计查找223596累计插入223596
4累计查找231000累计插入211000
2累计查找231000累计插入221760
2129765
1132.2926533222198
"""
import multiprocessing

from MAG_paper.tools import connect_to_table, timestamp

def upadate_age(a,b,pro_id):
    col1 = connect_to_table('wangwenbin', 'MAG_Physics_Cite')
    col2 = connect_to_table('wangwenbin', 'MAG_author_year')
    col3 = connect_to_table('wangwenbin', 'hhahh')

    papers = []
    suc = 0
    num = 0
    fail = 0

    for i in col1.find()[a:b]:
        num += 1
        authors = i.get('new_authors')
        pub_year = i.get('year')
        if authors and pub_year:
            new_authors = []
            for author in authors:
                new_author = {}
                author_id = author.get('id')
                new_author['author_id'] = author_id
                new_author['author_name'] = author.get('name')
                new_author['author_age'] = pub_year - col2.find_one({'_id': author_id})['first_year']
                new_authors.append(new_author)
            i['authors'] = new_authors
            papers.append(i)
            if len(papers) == 10000:
                col3.insert_many(papers)
                papers.clear()
                suc += 1
                print(str(pro_id)+' 查找' + str(num) + '插入' + str(suc * 10000))
        else:
            papers.append(i)
    col3.insert_many(papers)
    print(str(pro_id)+'累计查找' + str(num) + '累计插入' + str(len(papers)))
    papers.clear()
    print(fail)


if __name__ == '__main__':
    start = timestamp()

    p1 = multiprocessing.Process(target=upadate_age, args=(231000*0, 231000*1, 1))
    p2 = multiprocessing.Process(target=upadate_age, args=(231000*1, 231000*2, 2))
    p3 = multiprocessing.Process(target=upadate_age, args=(231000*2, 231000*3, 3))
    p4 = multiprocessing.Process(target=upadate_age, args=(231000*3, 231000*4, 4))
    p5 = multiprocessing.Process(target=upadate_age, args=(231000*4, 231000*5, 5))
    p6 = multiprocessing.Process(target=upadate_age, args=(231000*5, 231000*6, 6))
    p7 = multiprocessing.Process(target=upadate_age, args=(231000*6, 231000*7, 7))
    p8 = multiprocessing.Process(target=upadate_age, args=(231000*7, 231000*8, 8))
    p9 = multiprocessing.Process(target=upadate_age, args=(231000*8, 231000*9, 9))
    p10 = multiprocessing.Process(target=upadate_age, args=(231000*9, 231000*10, 10))

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
    end = timestamp()
    print(end - start)