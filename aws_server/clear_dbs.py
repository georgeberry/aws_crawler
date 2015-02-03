from pymongo import MongoClient

if __name__ == '__main__':
    mongo_cl = MongoClient('localhost', port=27017)
    db = mongo_cl['project2']

    db.drop_collection('queries')
    db.drop_collection('timelines')
    db.drop_collection('urls')
    db.drop_collection('profiles')
    db.drop_collection('friends')
    db.drop_collection('credentials')