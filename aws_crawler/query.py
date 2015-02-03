from __future__ import division
'''
crawler side

handles whatever type of query is requested
'''
from twitter import *
from time import time, sleep
from put_to_log_queue import PutToLogQueue
from email.utils import parsedate_tz
from datetime import datetime, timedelta
from calendar import timegm
from pymongo import MongoClient
from random import uniform
from math import ceil, floor
from copy import copy

class Query:
    '''
    queries twitter: obeys ratelim

    max_id = max tweet value; allows paging backwards
    since_id = min tweet value; pages forward

    need both:
        store since_id in mongo
        page backwards from the top using max_id for individual queries

    writes parsed tweet to mongo (should have unix timestamp)
    writes updated version of query to mongo

    '''

    def __init__(self, env):
        oauth_token = ''
        oauth_secret = ''
        consumer_key = ''
        consumer_secret = ''        

        self.logger = PutToLogQueue()

        #always localhost here because we've already set up tunnel
        mongo_cl = MongoClient('localhost', port=env.confs['mongo_port'])
        db = mongo_cl['project2']
        self.query_coll = db['queries']
        self.timeline_coll = db['timelines']
        self.url_coll = db['urls']
        self.profile_coll = db['profiles']
        self.friends_coll = db['friends']
        self.credentials = db['credentials']

        credentials = self.credentials.find_one({'name':env.instance_name})
        print credentials

        self.t = Twitter(
            auth = OAuth(
            credentials['access_token'],
            credentials['access_token_secret'],
            credentials['consumer_key'],
            credentials['consumer_secret'],
            )
        ) #credentials here

        print 'connected to collections'

        self.calls_left, self.reset_time = None, None


    def query_timeline(self, user_id, since_id = -1, journalist=False):
        '''
        gets timeline for one user

        writes a new query to mongo of form:
            { 'query_type': 'timeline',
            'user_id':user_id, 
            'since_id': new_since_id, 
            'next_query_timestamp': next_query_timestamp
            }
        bulk insert timeline tweets into mongo
            
        '''

        begin_time = time()

        if since_id == -1:
            since_id = None
        max_id = None #query-specific
        iterations = 0

        user_tweets = []
        user_profile = None

        print 'before timeline call'

        try:
            #calls_left, reset_time = self.remaining_calls(call_type='timeline')
            #print 'got calls'

            while True:
                #wait here if out of calls
                #self.wait_for_refresh(calls_left, reset_time)

                if self.calls_left == None and self.reset_time == None: #run this first time
                    self.calls_left, self.reset_time = self.remaining_calls(call_type='timeline')

                print 'BEGIN: {} calls remaining, refresh at {}'.format(self.calls_left, self.reset_time)

                if self.calls_left > 1: #go if we have calls
                    pass
                elif self.calls_left <= 1:
                    to_wait = self.reset_time - floor(time()) + 2
                    print 'SLEEPING: for {} with {} calls left'.format(to_wait, self.calls_left)
                    sleep(max(5, to_wait))

                    #check if calls are reset:
                    test_calls_left, test_reset_time = self.remaining_calls(call_type='timeline')
                    while test_calls_left < 10:
                        print 'still not refreshed, sleeping 30'
                        sleep(60)
                        test_calls_left, test_reset_time = self.remaining_calls(call_type='timeline')

                    self.calls_left, self.reset_time = test_calls_left, test_reset_time

                    print 'REFRESHED: continuing with {} calls, refresh {} seconds in the future'.format(self.calls_left, self.reset_time - floor(time()))
                    continue

                self.calls_left -= 1


                try:
                    if max_id: #for subsequent calls back in a timeline
                        if since_id: #maxid YES sinceid YES
                            tq = self.t.statuses.user_timeline(id=int(user_id), count=200, include_rts=True, max_id=int(max_id), since_id=int(since_id), trim_user=True)
                        else: #maxid YES sinceid NO
                            tq = self.t.statuses.user_timeline(id=int(user_id), count=200, include_rts=True, max_id=int(max_id), trim_user=True)
                    else: #first call to a timeline; get user profile
                        if since_id: #maxid NO sinceid YES
                            tq = self.t.statuses.user_timeline(id=int(user_id), count=200, include_rts=True, since_id=int(since_id))
                        else: #maxid NO sinceid NO
                            tq = self.t.statuses.user_timeline(id=int(user_id), count=200, include_rts=True)
                        if len(tq) > 0:
                            newest_tweet = tq[0] #update since_id on first tweet of query
                            new_since_id = newest_tweet['id']
                            user_profile = newest_tweet['user']
                        else:
                            user_profile = None
                            new_since_id = since_id

                    print 'query number {}'.format(iterations)

                    if len(tq) > 0:
                        parsed, max_id = self.parse_tweets(tq, journalist) #update max_id
                        max_id = max_id - 1 #decrement to start before last seen tweet
                        user_tweets.extend(parsed)
                    else:
                        self.logger.log_success('no new tweets')
                        #self.query_coll.update({'user_id':user_id}, {'$set': {'next_query_timestamp':time()+(86400*3), 'queried':False}})
                        break

                    #update iteration
                    iterations += 1

                    #loop break conditions
                    if iterations == 16:
                        self.logger.log_success('max iterations')
                        break #can't go back any farther
                    if len(tq) < 200: 
                        self.logger.log_success('less than 200')
                        break #end of timeline
                except Exception as e:
                    print str(e)
                    sleep(3)
                    if 'not authorized' in str(e).lower():
                        print '{} not authorized, removing from mongo'.format(user_id)
                        self.query_coll.remove({'user_id':user_id})
                        break
                    elif 'not found' in str(e).lower() or 'does not exist' in str(e).lower():
                        print '{} not found, removing from mongo'.format(user_id)
                        self.query_coll.remove({'user_id':user_id})
                        break
                    elif 'rate limit' in str(e).lower():
                        print 'RATELIM!'
                        sleep(10)
                        continue

            #have all tweets in timeline when we get here
            print 'finished querying'
            #when should we next check this user's tweets?
            if len(user_tweets) > 0:
                next_query_timestamp = self.next_query_time(user_tweets)
            else:
                next_query_timestamp = time() + float(86400*3)

            print 'got query timestamp'
            
            #new query to insert into mongo over the old one
            new_query = {
                'since_id': new_since_id, 
                'next_query_timestamp': next_query_timestamp,
                'queried': False
            }

            print new_query

            #write to mongo here
            #just update relevant items with new_query
            self.query_coll.update({'user_id':user_id}, {'$set':new_query})
            print 'updated query'

            #should ONLY insert tweets if they don't exist
            for tweet in user_tweets:
                self.timeline_coll.update({'id':tweet['id']}, {'$setOnInsert': tweet}, upsert=True)
            print 'wrote new tweets'
            if user_profile:
                self.profile_coll.update({'id':user_profile['id']}, {'$setOnInsert':user_profile}, upsert=True)
                print 'wrote profile info'

            self.logger.log_success('Successfully queried {} tweets from user {} in {} seconds!'.format(len(user_tweets), user_id, time()-begin_time))

        except Exception as e:
            self.logger.log_error(str(e))
            print 'second level exception: {}'.format(str(e))
            sleep(60)


    def remaining_calls(self, call_type):
        '''
        gets number of remaining calls
        '''
        if call_type == 'timeline':
            status = self.t.application.rate_limit_status()['resources']['statuses']['/statuses/user_timeline']
        elif call_type == 'friends':
            status = self.t.application.rate_limit_status()['resources']['friends']['/friends/ids']
        elif call_type == 'profile':
            status = self.t.application.rate_limit_status()['resources']['users']['/users/lookup']

        calls_left = int(status['remaining']) #remaining number of calls
        reset_time = int(status['reset']) #time of api reset

        return calls_left, reset_time


    def wait_for_refresh(self, calls_left, reset_time):
        '''
        sleeps until next call window
        '''
        if calls_left <= 5:
            now = time()
            to_wait = max(30, reset_time - now + 30) #15 seconds or difference
            print 'sleeping {}'.format(to_wait)
            sleep(to_wait)


    def parse_tweets(self, tweets, journalist):
        '''
        puts tweets in a regular list
        returns max_id for next query
        '''
        parsed = [self.parse_tweet(x, journalist) for x in tweets] #just make it a regular list; can add more parsing later
        max_id = min(x['id'] for x in parsed) #for paging backwards
        return parsed, max_id


    def parse_tweet(self, tweet, journalist):
        '''
        enforce some parsing rules:
            add "user_id" to tweet, allowing indexing
            throw out extra user info if exists
            add {'checked_for_important_items': False} 
        '''
        user_id = tweet['user']['id']
        tweet[u'user_id'] = user_id
        tweet[u'checked_for_important_items'] = False
        tweet[u'journalist'] = journalist

        if len(tweet['user']) > 3:
            tweet[u'user'] = {u'id':tweet['user']['id'], u'id_str':tweet['user']['id_str']}
        return tweet


    def next_query_time(self, parsed):
        ''' 
        computes time the next query for this user should happen
        '''
        max_time = max(self.twitter_to_unix(x['created_at']) for x in parsed)
        min_time = min(self.twitter_to_unix(x['created_at']) for x in parsed)

        print 'TIMES', max_time, min_time

        num_tweets = len(parsed)

        seconds_per_tweet = (max_time - min_time) / num_tweets

        #now + time for 200 tweets
        return time() + seconds_per_tweet * 200


    def twitter_to_unix(self, datestring):
        '''converts twitter format to unix timestamp. from:
        stackoverflow.com/questions/7703865/going-from-twitter
            -date-to-python-datetime-date'''
        time_tuple = parsedate_tz(datestring.strip())
        dt = datetime(*time_tuple[:6]) - timedelta(seconds=time_tuple[-1])
        return timegm(dt.timetuple())