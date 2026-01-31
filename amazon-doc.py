from concurrent.futures import ThreadPoolExecutor
import json
import os
import urllib.parse
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from urllib.request import urlretrieve
from pathlib import Path

import urllib
import pika

import argparse

parser = argparse.ArgumentParser(description="Example CLI tool")
parser.add_argument("command", help="Command --command index")

args = parser.parse_args()

print("Command : ", args.command)


# Connect to RabbitMQ
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost')
)
channel = connection.channel()

# Declare a queue (idempotent – safe to call multiple times)
channel.queue_declare(queue='amazon-doc')
channel.queue_declare(queue='amazon-doc-error')


def index():
    req = requests.get("https://docs.aws.amazon.com")
    req.raise_for_status()

    html = req.content
    soup = BeautifulSoup(html, "html.parser")

    input_val = (soup.find("input")["value"])   

    decoded = urllib.parse.unquote(input_val)

    #print(decoded)

    #with open("/tmp/amazon.xml", "a") as f:
    #    f.write(decoded)

    root = ET.fromstring(decoded)

    def send_to_kafka(url):
        if not url.startswith('http'):
            url = "https://docs.aws.amazon.com"+url
        channel.basic_publish(
            exchange='',
            routing_key='amazon-doc',
            body=url
            )
        print("SEND RABBITMQ")

    for item in root.findall(".//list-card-item"):
        href = (item.attrib['href'])
        print("Downloading",item.find('title').text.strip(),href)
        if(href.startswith('http')):
            print('Different domain',href)
            continue

        sub_req = requests.get("https://docs.aws.amazon.com"+href)
        sub_req.raise_for_status()

        sub_soup = BeautifulSoup(sub_req.content, "html.parser")

        if(not sub_soup.find("input")):
            print("failuere")
            continue

        sub_soup_input_val = (sub_soup.find("input")["value"])   

        sub_decoded = urllib.parse.unquote(sub_soup_input_val)
        print(sub_decoded)

        sub_root = ET.fromstring(sub_decoded)
        for link in sub_root.findall(".//simple-card"):
            if 'href' in link.attrib:
                send_to_kafka(link.attrib['href']);


def sync(url):
    url=url.split("#")[0]
    print("DOC downloading !",url)
    output_path = Path("./output/"+url)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not os.path.isfile(output_path):
        try:
            urlretrieve("https://docs.aws.amazon.com"+url, output_path)
        except Exception as e:
                print(e)


def callback(ch, method, properties, body):
    url = body.decode()
    try:
        if not url.endswith('.html'):
            req = requests.get(url)
            if req.status_code!=200:
                channel.basic_publish(
                    exchange='',
                    routing_key='amazon-doc-error',
                    body=url
                )
                return
            
            print(f" [x] Received {url}")

            html = str(req.content)
            key = 'var myDefaultPage = "'
            start = html.find(key);
            if start==-1:
                channel.basic_publish(
                    exchange='',
                    routing_key='amazon-doc-error',
                    body=url
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)
                print("KEY NOT FOUND !",url)
                return    
            end = html[start+len(key)+1:].find('"')
            red = html[start+len(key):start+len(key)+1+end]
            url+=red
            
        print(url)

        req = requests.get(url)
        if req.status_code !=200:
            print("ERROR",url)
            channel.basic_publish(
                    exchange='',
                    routing_key='amazon-doc-error',
                    body=url
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)

            return        
        html = req.content
        soup = BeautifulSoup(html, "html.parser")
        metas = (soup.find_all("meta"))
        for meta in metas:
            if 'name' in (meta.attrs) and meta.attrs['name']=='pdf':
                print("CONTENT ",meta.attrs['content'])
                sync(meta.attrs['content'])

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(e);
        channel.basic_publish(
                    exchange='',
                    routing_key='amazon-doc-error',
                    body=url
            )
        ch.basic_ack(delivery_tag=method.delivery_tag)

def download():
    channel.basic_consume(
        queue='amazon-doc',
        on_message_callback=callback,
        auto_ack=False
    )

    print(" [*] Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()

if args.command=='index':
    index()
elif args.command=='download':
    download()
else:
    raise "Ukwnown command !"

