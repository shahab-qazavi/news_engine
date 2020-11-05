from publics import db
from datetime import datetime
col_news = db()['news']
start = datetime.now()
print(col_news.count_documents({}))
for items in col_news.find({}, {'url_hash': 1}).sort('create_date', -1):
    url_hash_id = []
    for item in col_news.find({'url_hash': items['url_hash']}, {'_id': 1}):
        url_hash_id.append(item['_id'])

    if len(url_hash_id) > 0:
        if items['_id'] in url_hash_id:
            url_hash_id.remove(items['_id'])
        col_news.delete_many({'_id': {'$in': url_hash_id}})

print(col_news.count_documents({}))
print((datetime.now() - start).total_seconds())
