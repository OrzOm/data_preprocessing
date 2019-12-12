import pymongo, multiprocessing, os
from MAG_paper.tools import connect_to_table, timestamp


def get_sorted_citation(col5, author_id):
    papers = col5.find({'id': author_id})
    # 统计完后得到整个生涯时间表，分别计算每一年的影响力
    citations = {}
    # 得到这个作者的所有论文
    for paper in papers:
        impact = paper['impact']
        for year, citation in impact.items():
            # 对于每篇文章，如果这一年已经有了，那么就基础上加上今年的，如果没有，就创建新的键值对
            if not citations.get(year):
                citations[year] = citation
            else:
                citations[year] += citation
    # 上一步只是跟新了每年获得的总引用，还不是累加前几年的引用，并且考虑乱序
    citations = sorted([i for i in citations.items()])
    sum = 0
    new_citations = {}
    for k, v in citations:
        sum += v
        new_citations[k] = sum

    return new_citations

def count_author_total_impact(a, b, ids, process_id):
    col5 = connect_to_table('wangwenbin', 'author_impact')
    col6 = connect_to_table('wangwenbin', 'test')
    dict_list = []
    process_id = process_id
    count = 0
    success = 0
    already = 0

    for i in col5.find()[a:b].batch_size(10000):
        author_id = i.get('id')
        name = i.get('name')
        count += 1
        # 判断是否有统计过这个作者的信息了
        if not ids.get(author_id):
            ids[author_id] = 1
            new_citations = get_sorted_citation(col5, author_id)

            i.clear()
            i['author_id'] = author_id
            i['author_name'] = name
            i['impact_total'] = new_citations
            # a_paper = pymongo.UpdateOne({'id': authorid}, {"$set": {'name': name, 'impact_total': new_citations}}, upsert=True)
            dict_list.append(i)
            # 每1000条才跟新一次
            if len(dict_list) == 10000:
                col6.insert_many(dict_list)
                dict_list.clear()
                success += 1
                print('进程' + str(process_id) + ', 累计查找数据' + str(count)+'，写入 '+str(success*10000)+' 次')
        else:
            already += 1
    col6.insert_many(dict_list)
    print('进程' + str(process_id) + ', 累计查找数据 ' + str(count)+'，写入 '+str(success*10000+len(dict_list))+' 次')
    print('重复的作者' + str(already))

if __name__ == '__main__':
    start = timestamp()

    ids = multiprocessing.Manager().dict()

    p1 = multiprocessing.Process(target=count_author_total_impact, args=(1100000*0, 1100000*0+1100000, ids, 1))
    p2 = multiprocessing.Process(target=count_author_total_impact, args=(1100000*1, 1100000*1+1100000, ids, 2))
    p3 = multiprocessing.Process(target=count_author_total_impact, args=(1100000*2, 1100000*2+1100000, ids, 3))
    p4 = multiprocessing.Process(target=count_author_total_impact, args=(1100000*3, 1100000*3+1100000, ids, 4))
    p5 = multiprocessing.Process(target=count_author_total_impact, args=(1100000*4, 1100000*4+1100000, ids, 5))
    p6 = multiprocessing.Process(target=count_author_total_impact, args=(1100000*5, 1100000*5+1100000, ids, 6))
    p7 = multiprocessing.Process(target=count_author_total_impact, args=(1100000*6, 1100000*6+1100000, ids, 7))
    p8 = multiprocessing.Process(target=count_author_total_impact, args=(1100000*7, 1100000*7+1100000, ids, 8))
    p9 = multiprocessing.Process(target=count_author_total_impact, args=(1100000*8, 1100000*8+1100000, ids, 9))
    p10 = multiprocessing.Process(target=count_author_total_impact, args=(1100000*9, 1100000*9+1100000, ids, 10))

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
    print('耗时%d' % (end - start))
