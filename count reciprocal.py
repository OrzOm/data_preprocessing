from MAG_paper.tools import connect_to_table, timestamp
import multiprocessing


def count_reciprocal():
    coauthors = []

    col1 = connect_to_table('wangwenbin', 'MAG_HCP')
    for i in col1.find():
        print(i['id'], i['title'], i['authors'])
        authors = i.get("authors")
        for author in authors:
            author_id = author.get('author_id')
            result = col1.find({"authors.author_id": author_id})
            print('第%s个作者' % authors.index(author))
            # 获取他所有的发表论文
            for paper in result:
                print(paper['id'], paper['title'], paper['authors'])
                # 添加所有论文的作者
                coauthors.append(paper['authors'])


        print('---------------------------------------')


if __name__ == "__main__":
    start = timestamp()
    count_reciprocal()
    end = timestamp()
    print(end - start)