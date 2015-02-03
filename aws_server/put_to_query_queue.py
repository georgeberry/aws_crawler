'''
server side

puts new queries on queue
basically puts everything on queue with -1
puts stuff on queue where the "next_query_timestamp" item is less than current time
'''

import boto.sqs
import boto.utils
from boto.sqs.message import Message
import pickle
from time import time, sleep
from copy import copy
import pymongo

class PutToQueryQueue:
    def __init__(self):
        self.conn = boto.sqs.connect_to_region('',
            aws_access_key_id = '',
            aws_secret_access_key = ''
            )
        #connect to query queue
        self.queue = self.conn.get_queue('query_queue')
        print 'PTQQ init'

    def put_items(self, query_coll):
        '''
        puts items where timestamp is less than now and queried = False
        '''

        to_wait = 30
        try_num = 0

        while True:
            now = time()
            cursor = query_coll.find({'next_query_timestamp': {'$lt': now}, 'queried':False})
            print 'PTQQ cursor fetched'

            if cursor.count() > 0:
                break
            else:
                try_num += 1
                print 'PTQQ sleeping for {}'.format(try_num*to_wait)
                sleep(to_wait*try_num)

        for item in cursor:
            outbound = copy(item)
            outbound.pop('_id')
            m = Message()
            m.set_body(pickle.dumps(item))
            self.queue.write(m)
            query_coll.update({'_id':item['_id']}, {'$set':{'queried':True}})

        print 'PTQQ put items'
        print 'PTQQ updated query status'

    def put_highest_2k(self, query_coll):
        '''
        runs every 15 minutes; basically gets 2k most recent posts 
        '''

        print 'putting highest 2k'
        cursor = query_coll.find().sort('next_query_timestamp', pymongo.ASCENDING)[0:2000]

        for item in cursor:
            outbound = copy(item)
            outbound.pop('_id')
            m = Message()
            m.set_body(pickle.dumps(item))
            self.queue.write(m)
            query_coll.update({'_id':item['_id']}, {'$set':{'queried':True}})

        print 'put highest 2k'

    def put_all(self, query_coll):
        cursor = query_coll.find().sort('next_query_timestamp', pymongo.ASCENDING)

        for item in cursor:
            outbound = copy(item)
            outbound.pop('_id')
            m = Message()
            m.set_body(pickle.dumps(item))
            self.queue.write(m)
            query_coll.update({'_id':item['_id']}, {'$set':{'queried':True}})

