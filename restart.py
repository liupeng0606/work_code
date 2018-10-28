import pika
import requests
import json
from bs4 import BeautifulSoup
import redis
user_pwd = pika.PlainCredentials("myuser", "mypass")
conn = pika.BlockingConnection(pika.ConnectionParameters('localhost', credentials=user_pwd))
channel_html = conn.channel()
channel_seed = conn.channel()
channel_html.queue_declare(queue='html')
channel_seed.queue_declare(queue='seed')



#
def doing(ch,method,properties,url):

     ch.basic_ack(delivery_tag=method.delivery_tag)


def wait_msg():
    # 从html队列get数据
    channel_html.basic_consume(doing,queue="html",no_ack=False)#no_ack 标记是否要在消息处理后发送确认信息
    channel_html.start_consuming()

wait_msg()


