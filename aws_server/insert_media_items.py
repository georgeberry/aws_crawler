import pymongo
from time import sleep

class InsertMediaItems:
    '''
    does three things:
        1) adds users mentioned in timeline collection to query collection
        2) adds urls found in timeline collection to url collection
        3) flags existing tweets in timeline collection as 'checked_for_important_items': True

    '''


    def __init__(self):
        print "IMI init"

        pass

    def check_tweets(self, query_coll, timeline_coll, url_coll):
        self.query_coll = query_coll
        self.timeline_coll = timeline_coll
        self.url_coll = url_coll

        to_wait = 30
        try_num = 0

        while True:
            cursor = self.timeline_coll.find({'checked_for_important_items':False,'journalist':True})
            if cursor.count() > 0:
                break
            else:
                try_num += 1
                print 'IMI sleeping for {}'.format(3600)
                sleep(to_wait*try_num)



        for tweet in cursor:
            #self.insert_urls(tweet) #do for every tweet
            if tweet['journalist'] == True: #do only for mentions of journalists
                self.insert_new_queries(tweet)
            self.timeline_coll.update({'_id':tweet['_id']}, {'$set': {'checked_for_important_items': True}})

        print "Inserted items"



    def insert_new_queries(self, tweet):
        for mention in tweet['entities']['user_mentions']:
            mention_id = mention['id']

            #should only enter stuff if the user_id doesn't exist
            #remember, we don't modify existing
            #so we can hardcode these to journalist == False
            #doing so will not overwrite anything in the queries collection
            self.query_coll.update(
                {'user_id': mention_id},
                { '$setOnInsert': {
                    'query_type': 'timeline',
                    'user_id': mention_id,
                    'since_id': None,
                    'next_query_timestamp': -1,
                    'queried': False,
                    'journalist': False,
                    }
                }, upsert = True
            )

    def insert_urls(self, tweet):
        '''
        grabs urls and inserts into mongo
        '''
        user_id = tweet['user_id']

        for url in tweet['entities']['urls']:
            expanded_url = url['expanded_url']
            self.url_coll.update({'user_id':user_id}, {'$push': {'urls': expanded_url}}, upsert=True)
