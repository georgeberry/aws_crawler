'''
crawler side

puts logging info on queue (confirmation of success or error message)
'''

import boto.sqs
import boto.utils
from boto.sqs.message import Message
import pickle
from time import time

class PutToLogQueue:
    def __init__(self):
        self.conn = boto.sqs.connect_to_region('',
            aws_access_key_id = '',
            aws_secret_access_key = ''
            )
        #connect to query queue
        self.queue = self.conn.get_queue('log_queue')
        self.inst = boto.utils.get_instance_metadata()['instance-id']

    def put_item(self, arg):
        '''
        arg is of arbitrary type, although it must be short

        returns None if nothing on queue
        '''
        assert len(arg) < 10001 #has to be "short"

        m = Message()
        m.set_body(pickle.dumps(arg))
        self.queue.write(m)

    def log_error(self, error):
        assert type(error) is str, 'Log error must be string'
        print 'ERROR: ', error
        self.put_item('ERROR on {} at {}: '.format(self.inst, time()) + error)

    def log_success(self, success):
        assert type(success) is str, 'Log success must be string'
        print 'SUCCESS: ', success
        self.put_item('SUCCESS on {} at {}: '.format(self.inst, time()) + success)