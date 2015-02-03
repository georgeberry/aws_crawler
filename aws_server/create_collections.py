from pymongo import MongoClient
import pickle, json

class Connection:
    '''
    collections are of following forms:

    queries:
        {
        'query_type': str,
        'user_id': int,
        'since_id': int, (default to -1)
        'next_query_timestamp': int (default to -1)
        'journalist':Boolean
        }

    timelines:
        {
        just the stuff that comes in tweets, indexed by user_id
        we bring user_id up a level to do this
        }

    urls:
        {
        user_id: int
        urls: []
        }

    profiles:
        {
        the stuff in a twitter-generated profile, index by id
        }

    friends:
        {
        indexed by user_id
        user_id: [friends]
        }
    '''

    def __init__(self):
        self.connect()
        self.ensure_indices()

    def connect(self):
        self.mongo_cl = MongoClient('localhost', port=27017)
        self.db = self.mongo_cl['project2']
        self.query_coll = self.db['queries']
        self.timeline_coll = self.db['timelines']
        self.url_coll = self.db['urls']
        self.profile_coll = self.db['profiles']
        self.friends_coll = self.db['friends']

    def ensure_indices(self):
        self.query_coll.ensure_index('next_query_timestamp')
        self.timeline_coll.ensure_index('id') #by tweet id
        self.url_coll.ensure_index('user_id')
        self.profile_coll.ensure_index('user_id')
        self.friends_coll.ensure_index('user_id')

    def seed_timeline_queries(self, path):
        '''
        passed items must have the ['user']['id'] field
        '''
        with open(path, 'rb') as f:
            seed_list = json.load(f)

        #give it a dict of form: {'id':id, 'journalist':Bool}
        #really we only need update-on-insert
        for seed_node in seed_list:
            self.query_coll.update(
                {'user_id': seed_node['id']},
                { '$setOnInsert': {
                'query_type': 'timeline',
                'user_id': seed_node['id'],
                'since_id': None,
                'next_query_timestamp': -1,
                'queried':False,
                'journalist': seed_node['journalist']
                }}, upsert = True
            )

        print 'seeded'
        

