"""
累计10030s
"""
from MAG_paper.tools import connect_to_table, timestamp
import multiprocessing


def ageofref(a, b, process_id):
    col1 = connect_to_table('wangwenbin', 'MAG_PhyCite_V1')
    col2 = connect_to_table('oga_one', 'mag_paper')
    col3 = connect_to_table('wangwenbin', 'MAG_phycite_ref_v2')

    papers = []
    count = 0
    loss = 0
    process_id = process_id

    for i in col1.find()[a:b].batch_size(10000):
        cite_year = i.get('year')
        refs = i.get('references')

        if refs and cite_year:
            new_ref = []
            for ref_id in refs:
                ref_info = {}
                ref_info['ref_id'] = ref_id
                try:
                    original_paper = col2.find_one({'id': ref_id}, {'year': 1})
                    pub_year = original_paper.get('year')
                    ref_info['pub_year'] = pub_year
                    if pub_year:
                        age = cite_year - pub_year
                    else:
                        age = None
                    ref_info['ref_age'] = age
                    new_ref.append(ref_info)
                except:
                    print(ref_id)
            del i['references']
            i['references'] = new_ref
            papers.append(i)
            if len(papers) == 10000:
                col3.insert_many(papers)
                papers.clear()
                count += 1
                print(str(process_id)+'插入  '+str(count*10000))
                # ({'id': old_id}, {'$set': {'references': new_ref}}, upsert=True)
        else:
            loss += 1
            # papers.append(i)
    col3.insert_many(papers)
    print(str(process_id)+'插入  ' + str(count*10000+len(papers)))
    print(str(process_id)+'没有ref或year'+str(loss))


if __name__ == '__main__':
    a = timestamp()

    p1 = multiprocessing.Process(target=ageofref, args=(240000 * 0, 240000 * 0 + 240000, 1))
    p2 = multiprocessing.Process(target=ageofref, args=(240000 * 1, 240000 * 1 + 240000, 2))
    p3 = multiprocessing.Process(target=ageofref, args=(240000 * 2, 240000 * 2 + 240000, 3))
    p4 = multiprocessing.Process(target=ageofref, args=(240000 * 3, 240000 * 3 + 240000, 4))
    p5 = multiprocessing.Process(target=ageofref, args=(240000 * 4, 240000 * 4 + 240000, 5))
    p6 = multiprocessing.Process(target=ageofref, args=(240000 * 5, 240000 * 5 + 240000, 6))
    p7 = multiprocessing.Process(target=ageofref, args=(240000 * 6, 240000 * 6 + 240000, 7))
    p8 = multiprocessing.Process(target=ageofref, args=(240000 * 7, 240000 * 7 + 240000, 8))
    p9 = multiprocessing.Process(target=ageofref, args=(240000 * 8, 240000 * 8 + 240000, 9))
    p10 = multiprocessing.Process(target=ageofref, args=(240000 * 9, 240000 * 9 + 240000, 10))

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