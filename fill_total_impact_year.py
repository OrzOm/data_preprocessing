"""
查找2776447插入2776447
103.56385970115662
"""
from MAG_paper.tools import connect_to_table, timestamp

a =timestamp()
col = connect_to_table('wangwenbin', 'author_total_impact')
insert_col = connect_to_table('wangwenbin', 'author_total_impact_3')
a_box = []
num = 0
suc = 0

# for i in col.find({'author_id': "2809284922"}, {'impact_total': 1}):
for i in col.find():
    num += 1
    impact_total = i.get('impact_total')
    if impact_total:
        years = list(impact_total.keys())
        start = int(years[0])
        end = int(years[-1])
        flag = 0
        for year in range(start, end):
            year = str(year)
            if year not in years:
                impact_total[year] = impact_total[str(int(year)-1)]
                flag = 1
        if flag:
            i['impact_total'] = {}
            impact_total = sorted(impact_total.items())
            for k, v in impact_total:
                i['impact_total'][k] = v
    a_box.append(i)
    if len(a_box) == 10000:
        insert_col.insert_many(a_box)
        a_box.clear()
        suc += 1
        print('查找'+str(num)+'插入'+str(suc*10000))
insert_col.insert_many(a_box)
print('查找'+str(num)+'插入'+str(suc*10000+len(a_box)))
b = timestamp()
print(b-a)