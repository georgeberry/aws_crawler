'''
crawler side

checks queue, pops item

with messages; be careful of unicode only characters and :'s

message format: 
{   
    'query_type': 'timeline' OR 'profile' OR 'friends',
    'user_id': Int,
    'screen_name': Str
}
'''

import boto.sqs
from boto.sqs.message import Message
import pickle
from put_to_log_queue import PutToLogQueue
from time import sleep

class PopOffQueryQueue:
    def __init__(self):
        self.conn = boto.sqs.connect_to_region('',
            aws_access_key_id = '',
            aws_secret_access_key = ''
            )
        #connect to query queue
        self.queue = self.conn.get_queue('query_queue')
        self.logger = PutToLogQueue()

    def pop_item(self):
        '''
        pops and returns an item

        sleeps for awhile if nothing is on queue and tries again
        '''
        to_wait = 30
        try_num = 1
        m = []

        while True:
            m = self.queue.get_messages(1)
            assert len(m) <= 1, self.logger.log_error('Length of message from query queue greater than length 1')
            if len(m) == 1: #exit loop if we have a message
                break
            else:
                print 'POQQ sleeping for {}'.format(to_wait*try_num)
                try_num += 1
                sleep(to_wait*try_num)

        message = m[0]
        self.queue.delete_message(message)
        self.logger.log_success('Popped off query queue!')
        return pickle.loads(message._body)

