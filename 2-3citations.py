"""
查找2302596插入2302596 no_year 0
633.0544681549072
"""
from MAG_paper.tools import connect_to_table,timestamp
import multiprocessing

def count_3th_citation():
    col1 = connect_to_table('wangwenbin', 'MAG_Physics_Cite')
    col2 = connect_to_table('wangwenbin', 'hsajkh')
    
    array = []
    no_year = 0
    suc = 0
    num = 0

    for paper in col1.find().batch_size(1000):
        num += 1
        citations = paper.get('citation')
        pub_year = paper.get('year')
        if pub_year:
            total = 0
            first_3_citation = 0
            for i in range(5):
                total += citations.get(str(pub_year + i), 0)
                if i == 2:
                    first_3_citation += total
            print(first_3_citation, total)
            paper['3th_citation'] = first_3_citation
            paper['5th_citation'] = total
            array.append(paper)
            if len(array) == 10000:
                col2.insert_many(array)
                array.clear()
                suc += 1
                print('查找' + str(num) + '插入' + str(suc * 10000))
        else:
            no_year += 1
    col2.insert_many(array)
    print('查找' + str(num) + '插入' + str(suc * 10000 + len(array))+" no_year "+str(no_year))



if __name__ == "__main__":
    start = timestamp()
    count_3th_citation()
    # p1 = multiprocessing.Process(target=count_3th_citation, args=(166000*0, 166000*1, 1))
    # p2 = multiprocessing.Process(target=count_3th_citation, args=(166000*1, 166000*2, 2))
    # p3 = multiprocessing.Process(target=count_3th_citation, args=(166000*2, 166000*3, 3))
    # p4 = multiprocessing.Process(target=count_3th_citation, args=(166000*3, 166000*4, 4))
    # p5 = multiprocessing.Process(target=count_3th_citation, args=(166000*4, 166000*5, 5))
    # p6 = multiprocessing.Process(target=count_3th_citation, args=(166000*5, 166000*6, 6))
    # p7 = multiprocessing.Process(target=count_3th_citation, args=(166000*6, 166000*7, 7))
    # p8 = multiprocessing.Process(target=count_3th_citation, args=(166000*7, 166000*8, 8))
    # p9 = multiprocessing.Process(target=count_3th_citation, args=(166000*8, 166000*9, 9))
    # p10 = multiprocessing.Process(target=count_3th_citation, args=(166000*9, 166000*10, 10))
    #
    # p1.start()
    # p2.start()
    # p3.start()
    # p4.start()
    # p5.start()
    # p6.start()
    # p7.start()
    # p8.start()
    # p9.start()
    # p10.start()
    #
    # p1.join()
    # p2.join()
    # p3.join()
    # p4.join()
    # p5.join()
    # p6.join()
    # p7.join()
    # p8.join()
    # p9.join()
    # p10.join()
    end = timestamp()
    print(end-start)
