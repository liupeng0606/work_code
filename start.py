import pika
import hashlib
import redis
import json

from bs4 import BeautifulSoup
import uuid
import requests
import time

"""
1.先看看要不要把这个ｈｔｍｌ文件入库

2.解析该网页内的链接（ｓｅｅｄ）然后经过下面两步骤，看看是否满足入库
３．去掉该网页内队列内重复ｓｅｅｄ
４．判断是否出域名
"""



user_pwd = pika.PlainCredentials("myuser", "mypass")
conn = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=user_pwd))
channel_html = conn.channel()
channel_seed = conn.channel()
channel_html.queue_declare(queue='html')
channel_seed.queue_declare(queue='seed')



channel_html.basic_publish(exchange='',routing_key='seed',body="http://www.52duzhe.com/")