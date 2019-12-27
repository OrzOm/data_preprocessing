"""
搜索了文章  2302596  共有作者次数  10677114  author_list插入  2776429
355.14612555503845
"""
from MAG_paper.tools import connect_to_table, timestamp


def create_author_table():
    collection = connect_to_table('wangwenbin', 'MAG_Physics_Cite')
    col3 = connect_to_table('wangwenbin', 'phy_author_table')
    col3.delete_many({})

    authors_list = []
    count = 0
    auth_num = 0
    new_list = []

    for i in collection.find():
        count += 1
        authors = i.get('new_authors')
        if authors:
            authors_id = map(lambda author: author.get('id'), authors)
            for id in authors_id:
                auth_num += 1
                authors_list.append((id, 1))
            if count % 10000 == 0:
                print(count)
    print(len(authors_list))
    authors_list = set(authors_list)
    print(len(authors_list))
    for key, value in authors_list:
        new_author = {}
        new_author['_id'] = key
        new_list.append(new_author)
    authors_list.clear()
    col3.insert_many(new_list)
    print("搜索了文章  " + str(count) + '  共有作者次数  ' + str(auth_num) + '  author_list插入  ' + str(len(new_list)))
    new_list.clear()

if __name__ == '__main__':
    a = timestamp()
    create_author_table()
    b = timestamp()
    print(b - a)