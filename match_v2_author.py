"""
successful    2138006
fail     164590
耗时2763      分钟46
损失率：0.071
"""
import pymongo
from MAG_paper.tools import connect_to_table, timestamp

def match_authors_in_v2():
    success = 0
    fail = 0
    papers = []
    col1 = connect_to_table('wangwenbin', 'MAG_PhyCite_V1')
    col2 = connect_to_table('academic', 'mag_papers')

    for i in col1.find():
        _id = i['_id']
        normal_title = i['title'].lower().strip().replace('（', '(').replace('）', ')')
        r = col2.find_one({'normal_title': normal_title}, {'authors': 1, 'title': 1, 'venue': 1})
        if r:
            success += 1
            new_authors = r.get('authors', None)
            new_title = r.get('title', None)
            new_venue = r.get('venue', None)
            s = pymongo.UpdateOne({'_id': _id},
                                  {'$set': {'new_authors': new_authors, 'new_title': new_title,
                                            'new_venue': new_venue}},
                                  upsert=True)
            papers.append(s)
            if len(papers) == 100000:
                col1.bulk_write(papers)
                papers = []
        else:
            fail += 1
        if (fail + success) % 100000 == 0:
            print('   成功  ', success, '    失败    ', fail)
    col1.bulk_write(papers)
    print('successful   ', success)
    print('fail    ', fail)


if __name__ == '__main__':
    start = timestamp()
    match_authors_in_v2()
    end = timestamp()
    last = end - start
    print("耗时%d      分钟%d  " % (last, last/60))
