import requests
import time
import pymongo
import pymysql
from LiePinSpider.spider.config import database,hds,mongo_url,mongo_DB,mongo_table,host,mysqlDB,user,key
from pyquery import PyQuery as pq
def index_page_html(industry,cur_page,index_url):
    print('解析索引页：',industry,'curPage=',cur_page,':',index_url)
    try:
        response = requests.get(index_url, headers=hds, timeout=5)
        time.sleep(3)
        html = pq(response.text)
        return html
    except Exception:
        print('get index failed:',industry,'curPage=',cur_page,' ',index_url)
        time.sleep(5)
        index_page_html(industry,cur_page,index_url)
def get_industry_url(start_page_html):
    try:
        industry_lis = start_page_html('.search-conditions .short-dd li')
        industry_urls = {}
        for li in industry_lis.items():
            print('industry',li('span').text())
            for a in li('.sub-industry a').items():
                industry_urls[a.text()] = 'https://www.liepin.com' + a.attr.href
                print('sub-industry',a.text())
                print(industry_urls[a.text()])
        return industry_urls
    except Exception:
        print('get industry_index failed')
def get_next_page_url(industry,cur_page,index_html):
    try:
        selector = '.wrap .job-content .sojob-result .pagerbar a'
        items = index_html(selector).items()
        if items:
            next_page_url = ''
            for item in items:
                if item.text() == '下一页':
                    if item.attr('class') != 'disabled':
                        next_page_url = 'https://www.liepin.com' + item.attr.href
                    else:
                        next_page_url = None
        else:
            next_page_url = None
        return next_page_url
    except Exception:
        print('get next page failed',industry,'curPage=',cur_page,' ',index_html)
def get_detail_page_url(index_html):
    try:
        selector = '.sojob-list li'# .job-info h3 a'
        detail_url_list = []
        stopvalue = 0
        for item in index_html(selector).items():
            if item('.downgrade-search'):
                stopvalue = 1
                break
            else:
                detail_url = item('.job-info h3 a').attr.href
                if detail_url.find('https://www.liepin.com/job/') >= 0:
                    detail_url_list.append(detail_url)
                else:
                    pass
        detail_page_return = (detail_url_list,stopvalue)
        return detail_page_return
    except Exception:
        print('get detail page failed',index_html)
def get_detail_page_html(industry,cur_page,detail_page_url):
    try:
        response = requests.get(detail_page_url, headers=hds, timeout=5)
        time.sleep(3)
        html = pq(response.text)
        return html
    except Exception:
        print('get detail page failed:',industry,'curPage =',cur_page, detail_page_url)
def parse_detail_page(industry,detail_html):
    try:
        title = detail_html('.about-position .title-info h1').text()
        company = detail_html('.about-position .title-info h3').text()
        salary = detail_html('.about-position .job-title-left .job-item-title').text()  # .split()[0]
        position = detail_html('.about-position .job-title-left .basic-infor span').text()
        pubtime = detail_html('.about-position .job-item .basic-infor time').attr('title')
        qualification = detail_html('.about-position .job-item .job-qualifications span').text()
        tag_s = []
        lis = detail_html('div.tag-list span').items()
        if lis:
            for li in lis:
                tag_s.append(li.text())
        tag_list = ''
        for tag in tag_s:
            tag_list = tag_list + tag +','
        description = detail_html('.about-position div:nth-child(3) .content').text()
        industry = industry
        industry_detail = detail_html('.right-blcok-post .new-compintro li:nth-child(1)').text()
        companySize = detail_html('.right-blcok-post .new-compintro li:nth-child(2)').text()[5:]
        comAddress = detail_html('.right-blcok-post .new-compintro li:nth-child(3)').text()[5:]
        if detail_html('.title-info label').text() == '该职位已结束':
            is_end = 1
        else:
            is_end = 0
        data = {
            'JobTitle':title,
            'company':company,
            'salary':salary,
            'position':position,
            'PubTime':pubtime,
            'qualification':qualification,
            'tag_list':tag_list,
            'description':description,
            'industry':industry,
            'industry_detail':industry_detail,
            'companySize':companySize,
            'comAddress':comAddress,
            'is_end':is_end
        }
        return data
    except Exception:
        print('parse detail page failed',industry,detail_html)
client = pymongo.MongoClient(mongo_url)
mongoDB = client[mongo_DB]
def save_to_mongo(industry,cur_page,i,url,data):
    try:
        if mongoDB[mongo_table].insert(data):
            print("保存成功: ", industry, 'curPage=', cur_page, ',', i, ':', url)
            return  True
    except Exception:
        print('Failed',data['url'])

def save_to_mysql(industry,cur_page,i,url,data):
    try:
        sql = "INSERT INTO liepin(`JobTitle`,`company`,`salary`,`position`,`PubTime`,`qualification`,`tag_list`,\
        `description`,`industry`,`industry_detail`,`companySize`,`comAddress`,`is_end`) \
        VALUES ('%s', '%s', '%s', '%s', '%s','%s','%s','%s','%s','%s','%s','%s','%d' )" % \
              (str(data['JobTitle']),str(data['company']),str(data['salary']),str(data['position']),str(data['PubTime']),
               str(data['qualification']),str(data['tag_list']),str(data['description']),str(data['industry']),str(data['industry_detail']),
               str(data['companySize']),str(data['comAddress']),int(data['is_end']))
        cursor.excute(sql)
        db.commit()
        print("保存成功: ", industry, 'curPage=', cur_page, ',', i, ':', url)
    except:
        print('Failed', data['url'])
        db.rollback()
def loop_detail_page(industry,cur_page,detail_page_url_list):
    i = 1
    for url in detail_page_url_list :
        html = get_detail_page_html(industry,cur_page,url)
        data = parse_detail_page(industry,html)
        data['url'] = url
        if database == 'mongodb':
            save_to_mongo(industry,cur_page,i,url,data)
        else:
            save_to_mysql(industry,cur_page,i,url,data)
        i+=1

def loop_all_page(cur_page,industry,index_html):
    detail_page = get_detail_page_url(index_html)
    detail_page_url_list = detail_page[0]
    stopvalue = detail_page[1]
    loop_detail_page(industry,cur_page,detail_page_url_list)
    if stopvalue != 1:
        next_page_url = get_next_page_url(industry,cur_page,index_html)
        if next_page_url != None:
            next_page_html = index_page_html(industry,cur_page,next_page_url)
            cur_page += 1
            loop_all_page(cur_page,industry,next_page_html)
        else:
            pass
    print('complete:',industry)

def spider(parameter):
    industry = parameter['industry']
    industry_url = parameter['url']
    print(industry)
    cur_page = 1
    industry_html = index_page_html(industry,cur_page,industry_url)#获取分行业索引页，curpage=0

    loop_all_page(cur_page,industry,industry_html)
