#!/usr/bin/env python
# -*- coding: utf-8 -*-
print('Text reader started')
import sys
import os
sys.path.append('/home/shahab/dev/newshub')
sys.path.append('/home/oem/dev/newshub')
sys.path.append('/root/dev/newshub')
sys.path.append('/app')

from publics import db, es, es45, check_and_raise
from datetime import datetime
print(str(datetime.now()))
from bson import ObjectId
from queue import Queue
import threading
import subprocess
from crawl_engine import crawl_engine
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from copy import copy


# check_and_raise('elasticsearch')
# check_and_raise('mongod')

col_news = db()['news']
col_engine_instances = db()['engine_instances']
col_error_logs = db()['error_logs']

q = Queue()
thread_count = 23
error_count = 0
count = 0
origin_log_list = ''
update_one_lists = []
news_contents = 0
new_contents = 0
running = threading.Event()
news_count = 0
done = True
origin_error_count = 0


def do_work(item):
        global count
        global new_contents
        global origin_log_list
        global origin_error_count
        # global update_one_lists

        count += 1
        print(count)
        # print(item)
        item['mongo_id'] = str(item['_id'])
        del item['_id']
        # print(item)
        # print('before crawl')
        # print(item)
        status, news_text, news_html, log_list, _error_count = crawl_engine(item, engine_instance_id)
        # print('after crawl')
        origin_error_count += _error_count
        # for items in log_list:
        #     origin_log_list.append(items)
        origin_log_list = log_list
        item['status'] = status
        item['text'] = news_text
        item['html'] = str(news_html)
        item['text_reader_id'] = engine_instance_id
        if news_text is not None and news_text != '':
            new_contents += 1
        es().index(index='newshub', doc_type='news', body=item)
        # try:
        #     es45().index(index='newshub', doc_type='news', body=item)
        # except:
        #     pass
        # print(item['mongo_id'])
        # print(item)
        # print(col_news.update_one({'_id': ObjectId(item['mongo_id'])}, {'$set': {
        #     'status': status,
        #     'text': news_text,
        #     'html': str(news_html),
        # }}).raw_result)

        update_one_lists.append(UpdateOne({'_id': ObjectId(item['mongo_id'])}, {'$set': {
            'status': status,
            'text': news_text,
            'html': str(news_html),
        }}))


def worker():
    global done
    global new_contents
    global news_count
    global update_one_lists
    global running
    # print(news_count)
    while done:
        item = q.get()
        # print(item)
        # if item is not None:
            # print('before work')
            # print(item)
        do_work(item)
        # print('do work')
        q.task_done()
        # global count
        if count == news_count:
        # # # elif item is None:
            done = False
            running.set()
            sys.exit()

        #     global origin_log_list
        #     global origin_error_count
        #     durations = (datetime.now() - start).total_seconds()
        #
        #     print('len update one')
        #     print(len(update_one_lists))
        #     try:
        #         col_news.bulk_write(update_one_lists)
        #     except BulkWriteError as bwe:
        #         print(bwe)
        #     if len(origin_log_list) > 0:
        #         # logs_count = 0
        #         # insert_logs = []
        #         # for item in origin_log_list:
        #         #     insert_logs.append(item)
        #         #     logs_count += 1
        #         #     if logs_count == 100:
        #         #         col_error_logs.insert_many(insert_logs)
        #         #         insert_logs = []
        #         #         logs_count = 0
        #         col_error_logs.insert_many(origin_log_list)
        #         # print(origin_log_list)
        #     # print(origin_log_list)
        #     # print(len(origin_log_list))
        #     col_engine_instances.update_one({'_id': ObjectId(engine_instance_id)}, {'$set': {
        #         'duration': durations,
        #         'errors': origin_error_count,
        #         'source_links': '',
        #         'all_contents': count,
        #         'new_contents': new_contents
        #     }})
        #     subprocess.run(['pkill', '-f', 'text_reader.py'])

# def worker():
#     global done
#     while done:
#         item = q.get()
#         do_work(item)
#         q.task_done()
#         global count
#         global news_count
#
#         if count == news_count:
#             global error_count
#             done = False
#             durations = (datetime.now() - start).total_seconds()
#             col_engine_instances.update_one({'_id': ObjectId(engine_instance_id)}, {'$set': {
#                 'duration': durations,
#                 'errors': error_count,
#                 'source_links': count,
#                 'new_contents': ''
#             }})
#             subprocess.run(['pkill', '-f', 'text_reader.py'])


def run():
    for i in range(thread_count):
        t = threading.Thread(target=worker)
        t.daemon = True  # thread dies when main thread (only non-daemon thread) exits.
        t.killed = True
        t.start()

    if news_id == '':
        news_list = col_news.find({'status': 'summary'}).sort('create_date', -1)
    else:
        news_list = col_news.find({'_id': ObjectId(news_id)})
    global news_count
    running.set()
    q.empty()
    for item in news_list:
        news_count += 1
        item['title'] = item['title'].decode('utf-8')
        item['summary'] = item['summary'].decode('utf-8')
        item['url'] = item['url'].decode('utf-8')
        q.put(item)
    # q.put(None)

    q.join()



start = datetime.now()
source_links = 0

# if os.getenv('MONGO') is not None and os.getenv('ELASTIC') is not None:
engine_instance_id = str(col_engine_instances.insert_one({
    'type': 'text',
    'start_date': start,
    'duration': -1,
    'source_links': -1,
    'errors': -1,
    'all_contents': -1,
    'new_contents': -1,
}).inserted_id)


news_id = ''
if len(sys.argv) > 1:
    news_id = sys.argv[1]

run()

# print('len update one')
# print(len(update_one_lists))
if len(update_one_lists) > 0:
    try:
        col_news.bulk_write(update_one_lists)
    except BulkWriteError as bwe:
        # print('update one')
        print(bwe)

if len(origin_log_list) > 0:
    try:
        col_error_logs.bulk_write(origin_log_list)
    except BulkWriteError as err:
        # print('insert log')
        print(err)
    # logs_count = 0
    # insert_logs = []
    # for item in origin_log_list:
    #     insert_logs.append(item)
    #     logs_count += 1
    #     if logs_count == 20:
    #         print(len(insert_logs))
    #         col_error_logs.insert_many(insert_logs)
    #         insert_logs = []
    #         logs_count = 0

duration = (datetime.now() - start).total_seconds()
print('duration is : ', duration)
print(col_engine_instances.update_one({'_id': ObjectId(engine_instance_id)}, {'$set': {
    'duration': duration,
    'errors': origin_error_count,
    'source_links': '',
    'all_contents': count,
    'new_contents': new_contents
}}).raw_result)

# else:
#     if os.getenv('MONGO') is None:
#         print('Fatal error: You must supply MONGO environment variable with mongodb docker name')
#     if os.getenv('ELASTIC') is None:
#         print('Fatal error: You must supply ELASTIC environment variable with mongodb docker name')
