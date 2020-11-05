__author__ = 'Shahab Qazavi & Ehsan Shirzadi'
print('Link grabber started')
import sys
import os
sys.path.append('/home/shahab/dev/newshubmotors')
sys.path.append('/home/oem/dev/newshubmotors')
sys.path.append('/root/dev/newshubmotors')
sys.path.append('/app')
from publics import db, PrintException
import time
from datetime import datetime
from bson import ObjectId
import requests
import subprocess
from bs4 import BeautifulSoup
col_news = db()['news']
col_sources = db()['sources']
col_source_links = db()['source_links']
col_engine_instances = db()['engine_instances']
col_error_logs = db()['error_logs']
count = 0
news_count = 0
link_count = 0
count_mongo = 0
find_run_mongo = 0
find_mongo = 0
insert_mongo = 0
update_mongo = 0
from queue import Queue
import threading
q = Queue()
thread_count = 15
urls_hash_list = []
content_list = []
logs_list = []
done = True
import urllib3
import hashlib

# export PYTHONWARNINGS="ignore:Unverified HTTPS request"
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
running = threading.Event()


def create_md5(string):
    hash_object = hashlib.md5(string.encode())
    return hash_object.hexdigest()


def log_error(type, page_url, selector, data, error, source_id, source_link_id, engine_instance_id, module):
    try:
        global logs_list
        global error_count
        error_count += 1
        col_error_logs.insert_one({
            'engine_instance_id': str(engine_instance_id),
            'type': type,
            'source_id': source_id,
            'source_link_id': source_link_id,
            'page_url': page_url,
            'selector': selector,
            'data': data,
            'date': datetime.now(),
            'error': error,
        })
    except:
        PrintException()
        print('LOG PRODUCED LOG!')


def get_page(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    global logs_list
    global error_count
    try:
        # print(url)
        result = requests.get(url, headers=headers, verify=False)
    except:
        try:
            result = requests.get(url, verify=False)
        except:
            result = ''
            print('error in get page')
            logs_list.append(
                {'type': 'read_url', 'page_url': url, 'selector': '',
                 'data': {},
                 'error': PrintException(),
                 'source_link_id': '',
                 'engine_instance_id': engine_instance_id,
                 'module': 'link_grabber',
                 'date':datetime.now()})
            error_count += 1
            # log_error(type='get_url', page_url=url, selector='', data={},
            #           error=PrintException(), source_id='',
            #           source_link_id='', engine_instance_id=engine_instance_id,
            #           module='link_grabber')
    # f = open('temp.html', 'w')
    # f.write(result.text)
    # f.close()
    finally_result = ''
    if result is not None or result != '':
        finally_result = BeautifulSoup(result.text, 'html.parser')
    return finally_result


def do_work(item_info):
    global urls_hash_list
    global content_list
    global logs_list
    global count_mongo
    global error_count
    source_link = item_info['source_link']
    source = item_info['source']
    global count
    count += 1
    try:
        global link_count
        global new_contents
        link_count += 1
        print(link_count)
        html = get_page(source_link['url'])

        if html is not None or html != '':
            # print('html')
            for item in html.select(source_link['box']):
                try:
                    if source_link['link'] == '':
                        href = item['href']
                    else:
                        href = item.select(source_link['link'])
                        href = href[0]['href'] if len(href) != 0 else ''
                except:
                    PrintException()
                    href = ''
                    logs_list.append({'type':'extract_link', 'page_url':source_link['url'], 'selector':source_link['link'], 'data':{},
                                      'error':PrintException(), source_id:str(source['_id']),
                                      'source_link_id':str(source_link['_id']), 'engine_instance_id':engine_instance_id,
                                      'module':'link_grabber',
                                      'date':datetime.now()})
                    error_count += 1
                    # log_error(type='extract_link', page_url=source_link['url'], selector=source_link['link'], data={},
                    #           error=PrintException(), source_id=str(source['_id']),
                    #           source_link_id=str(source_link['_id']), engine_instance_id=engine_instance_id,
                    #           module='link_grabber')

                if href[:2] == '..': href = href.replace('..', '')
                # if href != '' and href[0] != '/': href = '/'+href
                # if col_news.count_documents({'url': source_link['base_url'] + item.select(source_link['link'])[0]['href']}) == 0:

                # col_counter = col_news.count_documents({'url': source_link['base_url'] + href})
                # count_mongo += 1
                # if col_counter == 0:
                try:
                    if source_link['base_url'] not in href:
                        url = source_link['base_url'] + href
                    else:
                        url = href
                except:
                    PrintException()
                    url = ''
                    logs_list.append(
                        {'type': 'read_url', 'page_url': source_link['url'], 'selector': '',
                         'data': {},
                         'error': PrintException(), source_id: str(source['_id']),
                         'source_link_id': str(source_link['_id']), 'engine_instance_id': engine_instance_id,
                         'module': 'link_grabber',
                         'date':datetime.now()})
                    error_count += 1
                    # log_error(type='read_url', page_url=source_link['url'], selector='', data={},
                    #           error=PrintException(), source_id=str(source['_id']),
                    #           source_link_id=str(source_link['_id']), engine_instance_id=engine_instance_id, module='link_grabber')
                if source_link['title'] != '':
                    try:
                            title = item.select(source_link['title'])
                            # if title is None or title != '':
                            #     print('empty title')
                            title = title[0].text.strip()
                            # if len(title) != 0 else ''

                    except:
                        try:
                            # print('title')
                            title = item.select('div.box_economic > h3 > a')[0].text
                        except:
                            try:
                                title = item.select('div.box_economic > h1 > a')[0].text
                            except:
                                # print('title nashod')
                                PrintException()
                                title = ''
                                logs_list.append(
                                    {'type': 'extract_title', 'page_url': source_link['url'], 'selector': source_link['title'],
                                     'data': {},
                                     'error': PrintException(), source_id: str(source['_id']),
                                     'source_link_id': str(source_link['_id']),
                                     'engine_instance_id': engine_instance_id,
                                     'module': 'link_grabber',
                                     'date':datetime.now()})
                                error_count += 1
                else:
                    title = ''
                            # log_error(type='extract_title', page_url=source_link['url'], selector=source_link['title'], data={},
                            #           error=PrintException(), source_id=str(source['_id']),
                            #           source_link_id=str(source_link['_id']), engine_instance_id=engine_instance_id, module='link_grabber')
                if source_link['summary'] != '':
                    try:
                        summary = item.select(source_link['summary'])
                        summary = summary[0].text.strip()
                        # if len(summary) != 0 else ''
                    except:
                        # print('summary error')
                        PrintException()
                        summary = ''
                        logs_list.append(
                            {'type': 'extract_summary', 'page_url': source_link['url'], 'selector': source_link['summary'],
                             'data': {},
                             'error': PrintException(), source_id: str(source['_id']),
                             'source_link_id': str(source_link['_id']),
                             'engine_instance_id': engine_instance_id,
                             'module': 'link_grabber',
                             'date':datetime.now()})
                    error_count += 1
                else:
                    summary = ''
                    # log_error(type='extract_summary', page_url=source_link['url'], selector=source_link['summary'],
                    #           data={}, error=PrintException(), source_id=str(source['_id']),
                    #           source_link_id=str(source_link['_id']), engine_instance_id=engine_instance_id, module='link_grabber')

                if source_link['date'] != '':
                    try:
                        date = item.select(source_link['date'])
                        date = date[0].text.strip()
                        # if len(date) != 0 else ''
                    except:
                        PrintException()
                        date = ''
                        logs_list.append(
                            {'type': 'extract_date', 'page_url': source_link['url'], 'selector': source_link['date'],
                             'data': {},
                             'error': PrintException(), source_id: str(source['_id']),
                             'source_link_id': str(source_link['_id']),
                             'engine_instance_id': engine_instance_id,
                             'module': 'link_grabber',
                             'date':datetime.now()})
                        error_count += 1
                else:
                    # print('date')
                    date = ''
                    # log_error(type='extract_date', page_url=source_link['url'], selector=source_link['date'], data={},
                    #           error=PrintException(), source_id=str(source['_id']),
                    #           source_link_id=str(source_link['_id']), engine_instance_id=engine_instance_id, module='link_grabber')
                if source_link['image'] != '':
                    try:
                        selected = item.select(source_link['image'])
                        try:
                            image = selected[0]['data-src']
                            # if len(selected) != 0 else ''
                        except:
                            image = selected[0]['src']
                        if 'http' not in image:
                            if image != '' and image[0] != '/': image = '/'+image
                            image = source_link['base_url']+image
                    except:
                        PrintException()
                        image = ''
                        logs_list.append(
                            {'type': 'extract_image', 'page_url': source_link['url'], 'selector': source_link['image'],
                             'data': {},
                             'error': PrintException(), source_id: str(source['_id']),
                             'source_link_id': str(source_link['_id']),
                             'engine_instance_id': engine_instance_id,
                             'module': 'link_grabber',
                             'date':datetime.now()})
                        error_count += 1
                else:
                    image = ''
                    # log_error(type='extract_image', page_url=source_link['url'], selector=source_link['image'],
                    #           data={}, error=PrintException(), source_id=str(source['_id']),
                    #           source_link_id=str(source_link['_id']), engine_instance_id=engine_instance_id, module='link_grabber')
                url_hash = create_md5(url)
                urls_hash_list.append(url_hash)
                new_contents += 1
                # if col_news.count_documents({'url_hash': url_hash}) == 0:
                content_list.append({
                    'source_id': str(source['_id']),
                    'link_grabber_id': engine_instance_id,
                    'source_link_id': str(source_link['_id']),
                    'source_name': source['name'],
                    'create_date': datetime.now(),
                    'last_update': datetime.now(),
                    'url': url.encode('utf-8'),
                    'url_hash': url_hash,
                    'title': title.encode('utf-8'),
                    'summary': summary.encode('utf-8'),
                    'date': date,
                    'source_url': source_link['url'],
                    'status': 'summary',
                    'image': image,
                    'text_selector': source_link['text'],
                    'category_id': source_link['category_id'] if 'category_id' in source_link else '',
                    'text': '',
                    'html': '',
                })
        else:
            print(html)
            logs_list.append(
                {'type': 'get html', 'page_url': source_link['url'], 'selector': source_link['image'],
                 'data': {},
                 'error': PrintException(), source_id: str(source['_id']),
                 'source_link_id': str(source_link['_id']),
                 'engine_instance_id': engine_instance_id,
                 'module': 'link_grabber',
                 'date':datetime.now()})
    except Exception as e:
        # print(e)
        logs_list.append(
            {'type': 'read_url', 'page_url': source_link['url'], 'selector': '',
             'data': {},
             'error': PrintException(), source_id: str(source['_id']),
             'source_link_id': str(source_link['_id']),
             'engine_instance_id': engine_instance_id,
             'module': 'link_grabber',
             'date':datetime.now()})
        # log_error(type='read_url', page_url=source_link['url'], selector='', data={}, error=PrintException(),
        #           source_id=str(source['_id']), source_link_id=str(source_link['_id']),
        #           engine_instance_id=engine_instance_id, module='link_grabber')


def worker(num):
    global done
    global running
    # global news_count
    # global link_count
    # global urls_hash_list
    # global content_list
    # global logs_list
    while done:
        item = q.get()
        if item is not None:
            do_work(item)

            # continue
            q.task_done()
        else:
            q.task_done()
            # q.empty()
            # q.get_nowait()
            # print(num)
            # quit()
            # exit()
            # print('after quit')

            done = False
            running.set()
            # os._exit(1)
            sys.exit()
    return True
        # q.task_done()
    # print('omad inja')


        # if link_count == news_count:
            # insert_mongo_list = []
            # for items in col_news.find({'url_hash': {'$in': urls_hash_list}}):
            #     if items['url_hash'] in urls_hash_list:
            #         urls_hash_list.remove(items['url_hash'])
            # for item in content_list:
            #     if item['url_hash'] in urls_hash_list:
            #         insert_mongo_list.append(item)

        # url_hash_removed_2 = []
            # url_hash_removed_2.append(45)

            # for items in col_news.find({'url_hash': {'$in': urls_hash_list}}, {'url_hash': 1}):
            #     # print(items)
            #     if items['url_hash'] in urls_hash_list:
            #         for item in urls_hash_list:
            #             if items['url_hash'] == item:
            #                 urls_hash_list.remove(items['url_hash'])
            #                 url_hash_removed.append(items['url_hash'])
            # print(len(content_list))
            # content_list = list(filter(lambda x: x['url_hash'] in urls_hash_list, content_list))
            # final_result_list = []
            # for items in content_list:
            #     for item in col_news.find({'url_hash': items['url_hash']}):
            #         print(item)
            #         # final_result_list.append(item)
            # print(len(content_list))
            # # print(len(final_result_list))
            # if len(content_list) > 0:
            #     try:
            #         print(col_news.insert_many(content_list))
            #     except Exception as e:
            #         print(e)
            # if len(logs_list) > 0:
            #     try:
            #         if len(logs_list) > 1000:
            #             logs_count = 0
            #             insert_logs = []
            #             for item in logs_list:
            #                 insert_logs.append(item)
            #                 logs_count += 1
            #                 if logs_count >= 1000:
            #                     col_error_logs.insert_many(insert_logs)
            #                     insert_logs = []
            #                     logs_count = 0
            #         else:
            #             col_error_logs.insert_many(logs_list)
            #     except Exception as e:
            #         print(e)
            # duration = (datetime.now() - start).total_seconds()
            # print(col_engine_instances.update_one({'_id': ObjectId(engine_instance_id)}, {'$set': {
            #     'duration': duration,
            #     'errors': error_count,
            #     'source_links': link_count,
            #     'new_contents': new_contents
            # }}).raw_result)
            # print(duration)
            # subprocess.run(['pkill', '-f', 'link_grabber.py'])


def run():
    global find_run_mongo
    threads = []
    for i in range(thread_count):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.daemon = True  # thread dies when main thread (only non-daemon thread) exits.
        t.setDaemon(True)
        t.start()

    if source_id == '':
        sources = col_sources.find({"enabled": True})
        find_run_mongo += 1
    else:
        sources = col_sources.find({"_id": ObjectId(source_id)})
        find_run_mongo += 1
    global news_count
    running.set()
    q.empty()
    col_source = col_source_links.find()
    find_run_mongo += 1
    source_list = []
    source_jsn = {}
    for source in sources:
        source_id_s = str(source['_id'])
        source_list.append(source_id_s)
        source_jsn[source_id_s] = source
    for source_link in col_source:
        if source_link['source_id'] in source_list:
            news_count += 1
            q.put({'source_link': source_link, 'source': source_jsn[source_link['source_id']]})
    q.put(None)

    q.join()


start = datetime.now()

# if os.getenv('MONGO') is not None and os.getenv('ELASTIC') is not None:
engine_instance_id = str(col_engine_instances.insert_one({
    'type': 'link',
    'start_date': start,
    'duration': -1,
    'source_links': -1,
    'errors': -1,
    'new_contents': -1,
}).inserted_id)
insert_mongo += 1

error_count = 0
new_contents = 0
print(len(sys.argv))
source_id = ''
if len(sys.argv) > 1:
    print(sys.argv[1])
    source_id = sys.argv[1]

run()

# print('exit run')


first_len_url_hash = len(urls_hash_list)
# print('len url hash')
# print(first_len_url_hash)

first_count_url_hash = col_news.count_documents({'url_hash': {'$in': urls_hash_list}})

# print('count url hash')
# print(first_count_url_hash)
# print('len content list')
# print(len(content_list))

urls_hash_list = list(set(urls_hash_list))
# urls_hash_list = list(urls_hash_list)
# print('after set')
# print(len(urls_hash_list))
for items in col_news.find({'url_hash': {'$in': urls_hash_list}}, {'url_hash': 1}):
    if items['url_hash'] in urls_hash_list:
        urls_hash_list.remove(items['url_hash'])
find_mongo += 1
        # urls_hash_list = list(filter((items['url_hash']).__ne__, urls_hash_list))
        # print('yes')
        # for item in urls_hash_list:

        #     if items['url_hash'] == item:
        #         urls_hash_list.remove(items['url_hash'])
        # url_hash_removed.append(items['url_hash'])

# print('after removed set')
# print(len(urls_hash_list))
# for item in urls_hash_list:
#     print(item)
# subprocess.run(['pkill', '-f', 'link_grabber.py'])
# len_removed_url = len(url_hash_removed)
# print('len removed')
# print(len_removed_url)
# print('len url hash')
# len_url_hash_after_remove = len(urls_hash_list)
# print(len_url_hash_after_remove)

len_content_list_before_remove = len(content_list)
# print('len content')
# print(len_content_list_before_remove)
final_list = []
for item in content_list:
    if item['url_hash'] in urls_hash_list:
        final_list.append(item)
        urls_hash_list.remove(item['url_hash'])
        # content_list = list(filter((item).__ne__, content_list))
        for items in content_list:
            if item['url_hash'] == items['url_hash']:
                content_list.remove(items)
                # content_list = list(filter((items['url_hash']).__ne__, content_list))

# content_list = list(filter(lambda x: x['url_hash'] in urls_hash_list, content_list))


len_content_list_after_remove = len(final_list)
# print('len content after remove')
# print(len_content_list_after_remove)

if len_content_list_after_remove > 0:
    try:
        col_news.insert_many(final_list)
        insert_mongo += 1
    except Exception as e:
        print('Error for insert content')
        print(e)

# print(len(logs_list))
len_logs_list = len(logs_list)
# print('len log list')
# print(len_logs_list)
if len_logs_list > 0:
    try:
        # if len_logs_list > 1000:
        #     # print('yes')
        #     logs_count = 0
        #     insert_logs = []
        #     for item in logs_list:
        #         insert_logs.append(item)
        #         logs_count += 1
        #         if logs_count == 1000:
        #             # print(len(insert_logs))
        #             print(col_error_logs.insert_many(insert_logs).acknowledged)
        #             # logs_list = list(list(list(set(logs_list) - set(insert_logs)) + list(set(insert_logs) - set(logs_list))))
        #
        #             insert_mongo += 1
        #             insert_logs = []
        #             logs_count = 0
        #     print('len log list')
        #     print(len(logs_list))
        # else:
        col_error_logs.insert_many(logs_list)
        insert_mongo += 1
    except Exception as e:
        print('Error for insert logs')
        print(e)

duration = (datetime.now() - start).total_seconds()
update_mongo += 1
print(col_engine_instances.update_one({'_id': ObjectId(engine_instance_id)}, {'$set': {
    'duration': duration,
    'errors': error_count,
    'source_links': link_count,
    'all_contents': new_contents,
    'new_contents': len_content_list_after_remove,
    ''
    'len_url_hash_before_remove': first_len_url_hash,
    'count_url_hash_mongodb': first_count_url_hash,
    # 'len_removed_url': len_removed_url,
    'len_url_hash_after_remove': len(urls_hash_list),
    'len_content_list_before_remove': len_content_list_before_remove,
    'len_content_list_after_remove': len_content_list_after_remove,
    'insert mongo': insert_mongo,
    'find mongo': find_mongo,
    'count mongo': count_mongo,
    'update mongo': update_mongo
}}).raw_result)
print(duration)
print({'insert mongo': insert_mongo,
      'find mongo': find_mongo,
      'find run mongo': find_run_mongo,
       'count mongo': count_mongo,
       'update mongo': update_mongo})
# else:
#     if os.getenv('MONGO') is None:
#         print('Fatal error: You must supply MONGO environment variable with mongodb docker name')
#     if os.getenv('ELASTIC') is None:
#         print('Fatal error: You must supply ELASTIC environment variable with mongodb docker name')
# Test Dev
