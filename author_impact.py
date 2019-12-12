"""
成功     2129765   失败（无作者）   172831
总数  2302596   作者数量   10677114
耗时   287.8089883327484
"""
from MAG_paper.tools import connect_to_table, timestamp

def count_author_citations():
    # 有作者的
    succes  = 0
    # 没作者的
    fail = 0
    # 总数
    num = 0
    # 累计作者数量
    authors_count = 0
    authors = []

    col1 = connect_to_table('wangwenbin', 'MAG_PhyCite_V1')
    col2 = connect_to_table('wangwenbin', 'author_impact_v2')

    for paper in col1.find({}, {'new_authors': 1, 'citation': 1, 'new_venue': 1}):
        aaa = {}
        num += 1
        new_authors = paper.get('new_authors')
        citations = paper.get('citation')
        new_venue = paper.get('new_venue')
        # 如果存在作者
        if new_authors:
            succes += 1
            for seq, author in enumerate(new_authors):
                authors_count += 1
                if seq == 0:
                    info = {}
                    for k, v in author.items():
                        info[k] = v
                    info['venue'] = new_venue
                    info['impact'] = {}
                    # r如果存在引用数量
                    if citations:
                        # 引用总数，并去掉总引用数量
                        sum = 0
                        del citations['total']
                        citations = sorted([i for i in citations.items()])
                        # del citations['total']
                        for year, citation in citations:
                            if year != 'total':
                                sum += citation
                                info['impact'][year] = sum
                    authors.append(info)
                    aaa = info['impact']
                else:
                    info = {}
                    for k, v in author.items():
                        info[k] = v
                    info['venue'] = new_venue
                    info['impact'] = aaa
                    authors.append(info)
        else:
            fail += 1
    # print(authors)
    # print(len(authors))
        if len(authors) == 100000:
            col2.insert_many(authors)
            print('成功     '+str(succes)+'   失败（无作者）   '+str(fail))
            authors.clear()
    col2.insert_many(authors)
    print('成功     '+str(succes)+'   失败（无作者）   '+str(fail))
    print('总数  '+str(num)+'   作者数量   '+str(authors_count))



if __name__ == "__main__":
    start = timestamp()
    count_author_citations()
    last = timestamp() - start
    print('耗时   '+str(last))