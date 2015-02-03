'''
server side

pops off logging queue and appends to logfile on disk
'''

import boto.sqs
from boto.sqs.message import Message
import pickle
from time import sleep

class PopOffLogQueue:
    def __init__(self):
        self.conn = boto.sqs.connect_to_region('',
            aws_access_key_id = '',
            aws_secret_access_key = ''
            )
        #connect to log queue
        self.queue = self.conn.get_queue('log_queue')
        self.log_path = 'log_queue.log'
        print 'POLQ init'

    def pop_items(self):
        '''
        pops item and writes to disk

        sleeps for awhile if nothing is on queue and tries again
        '''
        to_wait = 30
        try_num = 0
        m = []

        while True:
            m = self.queue.get_messages(10)
            if len(m) > 0: #exit loop if we have a message
                break
            else:
                try_num += 1
                print 'POLQ sleeping for {}'.format(to_wait*try_num)
                sleep(to_wait*try_num)

        print 'POLQ found messages'

        with open(self.log_path, 'a') as f:
            for message in m:
                self.queue.delete_message(message)
                f.write(pickle.loads(message._body) + '\n')

        print 'POLQ wrote'

