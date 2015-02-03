from pymongo import MongoClient
import json

if __name__ == '__main__':
    mongo_cl = MongoClient('localhost', port=27017)
    db = mongo_cl['project2']
    coll = db.credentials

    with open('data/credentials.json', 'rb') as f:
        from_disk = json.load(f)

    coll.remove({}) #clear all
    coll.insert(from_disk)