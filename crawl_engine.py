import sys
sys.path.append('/app')
import requests
from bs4 import BeautifulSoup
from publics import db, PrintException
from datetime import datetime
from bson import ObjectId
import urllib3
from pymongo import InsertOne
error_count = 0
log_list = []
col_engine_instances = db()['engine_instances']
col_error_logs = db()['error_logs']
col_source_links = db()['source_links']
col_news = db()['news']

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def remove_hrefs(html):
    for a in html.find_all('a'):
        del a['href']
    return html


def log(type, page_url, selector, data, error, source_id, engine_instance_id):
    # global error_count
    global log_list
    try:
        # from text_reader import error_count
        # error_count += 1
        # col_error_logs.insert_one({
        #     'engine_instance_id': str(engine_instance_id),
        #     'type': type,
        #     'source_id': source_id,
        #     'page_url': page_url,
        #     'selector': selector,
        #     'data': data,
        #     'date': datetime.now(),
        #     'error': error,
        # })
        log_list.append(InsertOne({
            'engine_instance_id': str(engine_instance_id),
            'type': type,
            'source_id': source_id,
            'page_url': page_url,
            'selector': selector,
            'data': data,
            'date': datetime.now(),
            'error': error,
        }))
    except:
        PrintException()


def crawl_engine(item, engine_instance_id):
    global log_list
    log_list = []
    news_html = None
    # print('inside crawl')
    # item['mongo_id'] = str(item['_id'])
    # del item['_id']
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 '
                             '(KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
#     headers = {
#     'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36',
# }
    status = ''
    news_text = ''
    # print('before req')
    # , headers = headers, verify = False
    try:
        # print('get')
        # print(item['url'])
        result = requests.get(url=item['url'], headers=headers, verify=False).status_code
        # print('after get')
    except:
        # print('inja')
        # print(e)
        try:
            # print('get sec')
            result = requests.get(item['url'], verify=False)
        except:
            status = 'error_get_url'
            result = ''
            log(type='get_url', page_url=item['url'], selector='', data={},
                error=PrintException(), engine_instance_id=engine_instance_id, source_id=item['source_id'])
    # print('after req')
    if (result != '') or (result is not None):
        try:
            # print('bs4')
            html = BeautifulSoup(result.text, 'html.parser')
        except Exception as e:
            html = ''
            status = 'error_bs4'
        try:
            # print('select')
            news_html = html.select(item['text_selector'])
            if len(news_html) > 0:
                news_html = news_html[0]
                try:
                    # print('remove')
                    news_html = remove_hrefs(news_html)
                except:
                    status = 'error_remove_href'
                status = 'text'
                source_link_info = col_source_links.find_one({'_id': ObjectId(item['source_link_id'])})
                if 'exclude' in source_link_info:
                    for exclude in source_link_info['exclude']:
                        for ex in news_html.select(exclude):
                            ex.decompose()
            else:
                status = 'Empty'
                log(type='read_text', page_url=item['url'], selector=item['text_selector'], data={},
                    error='Empty', engine_instance_id=engine_instance_id, source_id=item['source_id'])
        except Exception as e:
            news_html = ''
            status = 'error_selector'
            log(type='read_text', page_url=item['url'], selector=item['text_selector'], data={},
                error=str(e), engine_instance_id=engine_instance_id, source_id=item['source_id'])
            # col_news.update_one({'_id': ObjectId(item['mongo_id'])}, {'$set': {
            #     'status': 'error',
            #     'text': '',
            #     'html': '',
            #     'error': str(e),
            # }})
    if news_html is not None and news_html != '' and len(news_html) > 0:
        try:
            # print('text')
            news_text = news_html.text
        except:
            news_text = ''
            status = 'error_text'
            log(type='read_text', page_url=item['url'], selector=item['text_selector'], data={},
                error=PrintException(), engine_instance_id=engine_instance_id, source_id=item['source_id'])

    return status, news_text, news_html, log_list, error_count

