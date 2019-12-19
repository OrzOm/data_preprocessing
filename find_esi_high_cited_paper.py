from MAG_paper.tools import connect_to_table, timestamp
import math, multiprocessing

# def find_hcp(a, b, proc_id):
#     col1 = connect_to_table('wangwenbin', 'MAG_Physics_Cite')
#     col2 = connect_to_table('wangwenbin', 'mag_phy_top')
# 
#     papers = []
#     suc = 0
#     num = 0
# 
#     for i in col1.find()[a:b]:
#         num += 1
#         pub_year = i.get('year')
#         citaton = i.get('citation')
#         ten_year_citation = 0
#         if pub_year and citaton:
#             year_cite = sorted(citaton.items())
#             for year, cite in year_cite:
#                 if int(year) < int(pub_year)+10:
#                     ten_year_citation += cite
#         i['10th_citation'] = ten_year_citation
#         papers.append(i)
#         if len(papers) == 10000:
#             col2.insert_many(papers)
#             papers.clear()
#             suc += 1
#             print(str(proc_id)+'搜索' + str(num) + '插入' + str(suc * 10000))
#     col2.insert_many(papers)
#     print(str(proc_id)+'搜索' + str(num) + '插入' + str(suc * 10000 + len(papers)))


def find_hcp(a, b, proc_id):
    col1 = connect_to_table('wangwenbin', 'mag_phy_top')
    col2 = connect_to_table('wangwenbin', 'HCP')

    col2.remove({})
    papers = []

    for year in range(a, b):
        total = col1.find({'year': year}).count()
        top = math.ceil(total*0.01)
        num = 0
        for i in col1.find({'year': year}).sort([('10th_citation', -1)]):
            num += 1
            if num <= top:
                i['HCP'] = True
            else:
                i['HCP'] = False
            papers.append(i)
        col2.insert_many(papers)
        print('进程'+str(proc_id)+'   年份  '+str(year)+' 共计 '+str(total)+'  更新了  '+str(len(papers))+'  其中hcp  '+str(top))
        papers.clear()

if __name__ == '__main__':
    start = timestamp()
   
    p1 = multiprocessing.Process(target=find_hcp, args=(1957,  1962, 1))
    p2 = multiprocessing.Process(target=find_hcp, args=(1962,  1967, 2))
    p3 = multiprocessing.Process(target=find_hcp, args=(1967,  1972, 3))
    p4 = multiprocessing.Process(target=find_hcp, args=(1972,  1977, 4))
    p5 = multiprocessing.Process(target=find_hcp, args=(1977,  1982, 5))
    p6 = multiprocessing.Process(target=find_hcp, args=(1982,  1987, 6))
    p7 = multiprocessing.Process(target=find_hcp, args=(1987,  1992, 7))
    p8 = multiprocessing.Process(target=find_hcp, args=(1992,  1997, 8))
    p9 = multiprocessing.Process(target=find_hcp, args=(1997,  2002, 9))
    p10 = multiprocessing.Process(target=find_hcp, args=(2002,  2009, 10))
  
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

    print('第2阶段插入结束')
    end = timestamp()
    print('用时'+str(end-start))