from MAG_paper.tools import connect_to_table, timestamp


def fill_total_ciations():
    col1 = connect_to_table('wangwenbin', 'MAG_Physics_Cite')
    col2 = connect_to_table('wangwenbin', 'total_citations')
    col2.remove({})
    papers = []
    num = 0
    modify = 0
    suc = 0

    for i in col1.find():
        num += 1
        flag = 0
        citation = i.get('citation')
        if citation:
            i['total_citation'] = citation['total']
            del citation['total']
            years = list(citation.keys())
            # citations = sorted(citations.items())
            start = int(min(years))
            end = int(max(years))
            for year in range(start, end):
                year = str(year)
                if year not in years:
                    citation[year] = 0
                    flag = 1
            if flag:
                modify += 1
            papers.append(i)
        if len(papers) == 10000:
            col2.insert_many(papers)
            papers.clear()
            suc += 1
            print('搜索'+str(num)+'修改'+str(modify)+'插入'+str(suc*10000))
    col2.insert_many(papers)
    print('搜索' + str(num) + '修改' + str(modify) + '插入' + str(suc * 10000+len(papers)))


if __name__ == '__main__':
    a = timestamp()
    fill_total_ciations()
    b= timestamp()
    print(b-a)
