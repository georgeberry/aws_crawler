import boto.ec2
import boto.utils
import pymongo
from fabric.api import local
from put_to_log_queue import PutToLogQueue
from pop_off_query_queue import PopOffQueryQueue
from query import Query
from crawler_env import CrawlerEnv

def go():

    def ssh_tunnel(logger, env):

        if not env.test: #ssh tunnel if not a test!
            mongo_host = env.confs['mongo_host']
            mongo_port = env.confs['mongo_port']

            if mongo_host:
                local('autossh -M 0 -f -L{}:127.0.0.1:27017 -p 22 -N -f ubuntu@{} AUTOSSH_MAXSTART 50'.format(mongo_port, mongo_host))
                logger.log_success('Found mongo hostname in fabfile.py on')
            else:
                logger.log_error('No mongo hostname found in fabfile.py')
                raise ValueError, 'Must provide a mongo hostname'
        else:
            pass

    def query_loop(logger, env):
        poqq = PopOffQueryQueue()
        q = Query(env)

        try:
            while True:
                query_message = poqq.pop_item() #{query_type:'', user_id:''...}
                query_type = query_message['query_type']
                user_id = query_message['user_id']
                since_id = query_message['since_id']
                journalist = query_message['journalist']

                #route messages here
                if query_type == 'timeline':
                    q.query_timeline(user_id=user_id, since_id=since_id, journalist=journalist)
                if query_type == 'friends':
                    pass
                if query_type == 'profile':
                    pass

        except Exception as e:
            print 'Error is: ', e
            logger.log_error(str(e))


    env = CrawlerEnv(test=False) #set to test on local or connect to mongo
    print env.confs, env.test
    logger = PutToLogQueue()
    print 'logger up'
    ssh_tunnel(logger, env) #open the ssh tunnel in prod
    print 'ssh open'
    print 'querying'
    query_loop(logger, env)