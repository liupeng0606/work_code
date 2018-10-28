import pika
import redis
import json
from pymongo import MongoClient
from bs4 import BeautifulSoup
from functools import reduce
"""
1.先看看要不要把这个ｈｔｍｌ文件入库
2.解析该网页内的链接（ｓｅｅｄ）然后经过下面两步骤，看看是否满足入库
３．去掉该网页内队列内重复ｓｅｅｄ
４．判断是否出域名
"""

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
r = redis.Redis(connection_pool=pool)
user_pwd = pika.PlainCredentials("myuser", "mypass")
conn = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=user_pwd))
channel_html = conn.channel()
channel_seed = conn.channel()
channel_html.queue_declare(queue='html')
channel_seed.queue_declare(queue='seed')


db = MongoClient('127.0.0.1', 27017).duzhe
collection_url = db.url
collection_item = db.item

def get_meta_data(html):
    soup = BeautifulSoup(html, 'html.parser')
    seed_list = []
    for k in soup.find_all('meta'):
        try:
            seed_list.append(k['content'])
        except:
            pass
    if(len(seed_list)<1):
        return "201012"
    return seed_list[1]



def get_content(html):
    soup = BeautifulSoup(html, 'html.parser')
    p=""
    for k in soup.find_all('p'):
        p=p+str(k.text);
    return p

def get_auther(html):
    soup = BeautifulSoup(html, 'html.parser')
    auther_tag=soup.find("span",id="pub_date")
    if (auther_tag == None):
        old_auther_tag = soup.find("div", class_="title tcright")
        if (old_auther_tag == None):
            return "unKnow"
        else:
            auther=old_auther_tag.text
            return auther[auther.find("：") + 1:auther.find("来")].strip()
    else:
        auther=auther_tag.text
    return auther[auther.find("：")+1:].strip()

def get_source(html):
    soup = BeautifulSoup(html, 'html.parser')
    auther_tag=soup.find("span",id="media_name")
    if (auther_tag == None):
        old_auther_tag = soup.find("div", class_="title tcright")
        if(old_auther_tag==None):
            return "unknow"
        else:
            auther = old_auther_tag.text
            return auther[auther.find("源") + 2:].strip()
        return "unknow"
    else:
        auther=auther_tag.text
    return auther[auther.find("：")+1:].strip()



def get_type(html):
    soup = BeautifulSoup(html, 'html.parser')
    p_type=soup.find("div",class_="menuItem itemNow")
    if(p_type==None):
        old_p_type=soup.find("span",class_="category")
        if(old_p_type==None):
            return "unknow"
        else:
            return old_p_type.text.strip()
    h2_tage=p_type.find_previous("h2")
    if(h2_tage!=None):
        return h2_tage.text.strip()
    else:
        return "unknow"




def get_year(html):
    soup = BeautifulSoup(html, 'html.parser')
    href_tag=soup.find("a",href="index.html")
    if(href_tag==None):
        meta_data = get_meta_data(html)
        old_year_d_list=list(filter(lambda x:x.isdigit(),meta_data))
        if(len(old_year_d_list)<4):
            return "2010"
        return reduce(lambda x,y:str(x)+str(y),old_year_d_list[0:4])
    else:
        date=href_tag.text
    return date[0:4].strip()

def get_d(html):
    soup = BeautifulSoup(html, 'html.parser')
    date_tag=soup.find("a",href="index.html")
    if (date_tag == None):
        href_tag = soup.find("a", href="index.html")
        if (href_tag == None):
            meta_data = get_meta_data(html)
            old_year_d_list = list(filter(lambda x: x.isdigit(), meta_data))
            if (len(old_year_d_list) < 4):
                return "2010"
            return reduce(lambda x, y: str(x) + str(y), old_year_d_list[4:6])
    else:
        date=date_tag.text
    return date[6:8]


##-------------------------end mata data--------------------------------------------------

def get_title(html):
    soup = BeautifulSoup(html, 'html.parser')
    title_list=[]
    for k in soup.find_all('h1'):
        title_list.append(k.text)
    return title_list[0].strip()

def filter_seeds(item):
    if(item=="#"):
        return False
    if("http://" in item):
        return False
    return True

# 拆分url方便做判断，该方法与业务逻辑无关
def url2list(url):
    templist = url.split("/")
    url_list=list(templist)
    return list(filter(None,url_list))

# 控制要不要解析该url的html内容
def html_need2db(html):
    if(("作者" in html) and ("上一篇" in html)
            and ("下一篇" in html)  and ("来源" in html)):
        return True
    else:
        return False

# 解析html
def parse_html2db(html):
    title=get_title(html)
    collection_item.insert({"title":title.replace("."," "),"content":get_content(html),
                   "year":get_year(html),
                   "year_d":get_d(html),
                   "cat":get_type(html),
                   "auther":get_auther(html),
                   "source":get_source(html)
                   })
    print(title + "　的内容已经入库")
# 得到网页的seed
def get_html_seed(url,html):
    soup = BeautifulSoup(html, 'html.parser')
    seed_list = []
    for k in soup.find_all('a'):
        try:
           seed_list.append(k['href'])
        except:
            pass
    href_list= list(filter(filter_seeds, seed_list))
    result=list(map(lambda x:url[0:url.rfind('/')+1]+x,href_list))
    return result

# 控制不要爬出域名外的内容
def is_in_domain(url):
    if("http://www.52duzhe.com" in url):
       return True
    else:
        return False

# 控制不能重复添加url
def url_in_db(url):
    url = str(url).replace(".", " ")
    result = collection_url.find_one({"url":url})
    if(result==None):
        collection_url.insert({"url": url})
        return True
    else:
        return False

def doing(ch,method,properties,body):
    """根据url判断是否入库，从页面提取url放入seed队列"""
    body=str(body, 'utf-8')
    url_html_list=json.loads(body)
    body_url=url_html_list[0]
    body_html = url_html_list[1]
    is2db= html_need2db(body_html)
    if (is2db):
        parse_html2db(body_html)

    seed_list=get_html_seed(body_url,body_html)
    print(seed_list)
    no_rep_url = list(filter(url_in_db,seed_list))
    print(no_rep_url)
    result = list(filter(is_in_domain, no_rep_url))
    print(body_url+"内的可用链接为：")
    print(result)
    for i in result:
        channel_html.basic_publish(exchange='', routing_key='seed', body=i)
    ch.basic_ack(delivery_tag=method.delivery_tag)
def wait_msg():
    channel_html.basic_qos(prefetch_count=1)
    # 从html队列get数据
    channel_html.basic_consume(doing,queue="html",no_ack=False)#no_ack 标记是否要在消息处理后发送确认信息
    channel_html.start_consuming()

wait_msg()