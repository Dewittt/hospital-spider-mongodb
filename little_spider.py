from pyquery import PyQuery as pq
import requests
from multiprocessing import Pool
import pymongo

client = pymongo.MongoClient('localhost')
db = client['hospital']


def get_page(i):
    try:
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'
        }
        url = 'https://db.yaozh.com/hmap/' + str(i) + '.html'
        doc = pq(requests.get(url,headers = headers).text)
        parse_page(doc)
        print('获取第',i,'页成功')
    except Exception:
        print('爬取第',i,'页失败')

def parse_page(doc):
    try:
        items = doc('body > div.main > div.body.detail-main.no-side > div.table-wrapper > table > tbody').items()
        item_name = []
        item_detail = []
        for item in items:
            item_name.append(item.find('.detail-table-th').text())
            item_detail.append(item.find('.toFindImg').text())
            item_detail.append(' ')
            item_name.append(' ')
        new_item_name = list(str(item_name).split(' '))
        new_item_detail = list(str(item_detail).split(' '))
        dic = dict(zip(new_item_name, new_item_detail))
        save_mongo(dic)
    except Exception:
        print('爬取失败')

def save_mongo(dic):
    if db['history_detail'].insert(dic):
        print("保存成功")
    else:
        print('保存失败')


def main(i):
    get_page(i)

if __name__ == '__main__':
    pool = Pool()
    pool.map(main,[ i for i in range(1,64065)])
    pool.close()
    pool.join()
