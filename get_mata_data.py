import pika
import redis
import json
from pymongo import MongoClient
from bs4 import BeautifulSoup
import requests
import re
from functools import reduce

html = requests.get("http://www.52duzhe.com/1_2010_24/131.html", timeout=3)
html.encoding="gb2312"
content = html.text



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
            return "unknow"
        else:
            auther=old_auther_tag.text
            return auther[auther.find("：") + 1:auther.find("来")].strip()
    else:
        auther=auther_tag.text
    return auther[auther.find("：")+1:]

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
    return auther[auther.find("：")+1:]



def get_type(html):
    soup = BeautifulSoup(html, 'html.parser')
    p_type=soup.find("div",class_="menuItem itemNow")
    if(p_type==None):
        old_p_type=soup.find("span",class_="category")
        if(old_p_type==None):
            return "unknow"
        else:
            return old_p_type.text
        return "unknow"
    p_type=p_type.find_previous("h2")
    if(p_type!=None):
        return p_type.text
    else:
        return "unknow"
    return p_type





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


print(get_d(content))
# old_href_text="<a href=http://www.52duzhe.com/><img src=../Images/logo.jpg/></a>"
# old_year_d=list(filter(lambda x:x.isdigit(),old_href_text))
# print(old_year_d)
