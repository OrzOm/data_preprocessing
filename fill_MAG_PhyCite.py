from MAG_paper.tools import connect_to_table, timestamp


def fillMAG(oga_mag, mag_cite):
    num = 0
    find = 0
    for paper in mag_cite.find().batch_size(50):
        find += 1
        paper_id = paper['id']
        info = oga_mag.find_one({"id": paper_id})
        if info:
            num += 1
            del info['_id']
            mag_cite.update_one(
                {'id':paper_id},
                {'$set':info},
            )

    print('共找到%d条，更新%d条' % (find, num))


if __name__ == "__main__":
    start = timestamp()
    col1 = connect_to_table('oga_one', 'mag_paper')

    col2 = connect_to_table('wangwenbin', 'MAG_PhyCite_V1')

    fillMAG(col1, col2)

    end = timestamp()
    print('耗时%d, 合计%d分钟' % (end-start, (end-start)/60))