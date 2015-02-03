import boto.ec2
import boto.utils
from fabric.api import local
from pop_off_log_queue import PopOffLogQueue
from put_to_query_queue import PutToQueryQueue
from insert_media_items import InsertMediaItems
from create_collections import Connection
import threading
from time import sleep

def put_all():
    conn = Connection()
    ptqq = PutToQueryQueue()
    ptqq.put_all(conn.query_coll)

def go():
    timeline_path = '/home/ubuntu/prod_crawl/aws_server/data/seed_nodes.json'

    def put(ptqq, query_coll):
        while True:
            #ptqq.put_highest2k(query_coll)
            ptqq.put_items(query_coll)
            sleep(600)

    def pop(polq):
        while True:
            polq.pop_items()
            sleep(.5)

    def info(imi, query_coll, timeline_coll, url_coll):
        while True:
            imi.check_tweets(query_coll, timeline_coll, url_coll)
            sleep(2)

    conn = Connection()
    conn.seed_timeline_queries(timeline_path)

    ptqq = PutToQueryQueue()
    polq = PopOffLogQueue()
    imi = InsertMediaItems() #takes old items 

    put_loop = threading.Thread(target=put, args=(ptqq, conn.query_coll))
    pop_loop = threading.Thread(target=pop, args=(polq,))
    info_loop = threading.Thread(target=info, args=(imi, conn.query_coll, conn.timeline_coll, conn.url_coll))

    put_loop.daemon = True
    pop_loop.daemon = True
    info_loop.daemon = True

    put_loop.start()
    pop_loop.start()
    info_loop.start()

    #keep main thread alive
    while True:
        sleep(2)
