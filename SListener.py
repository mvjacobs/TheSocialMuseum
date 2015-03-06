__author__ = 'marc'

from tweepy import StreamListener
import json, time, sys
from pymongo import MongoClient


class SListener(StreamListener):
    def __init__(self, api=None, fprefix='streamer', ou_type='json'):
        self.api = api
        self.counter = 0
        self.fprefix = fprefix
        self.ou_type = ou_type
        self.output = open(time.strftime('data/%Y%m%d-%H%M%S') + '.json', 'w')
        self.delout = open('data/delete.txt', 'a')

    def on_data(self, data):

        if 'in_reply_to_status' in data:
            self.on_status(data)
        elif 'delete' in data:
            delete = json.loads(data)['delete']['status']
            if self.on_delete(delete['id'], delete['user_id']) is False:
                return False
        elif 'limit' in data:
            if self.on_limit(json.loads(data)['limit']['track']) is False:
                return False
        elif 'warning' in data:
            warning = json.loads(data)['warnings']
            print warning['message']
            return False

    def on_status(self, status):
        if self.ou_type == 'mongo':
            write_to_mongo("localhost", 27017, "museums", "museum_tweets", status)
        elif self.ou_type == 'json':
            self.counter += 1

            if self.counter >= 20000:
                self.output.close()
                self.output = open(time.strftime('data/%Y%m%d-%H%M%S') + '.json', 'w')
                self.counter = 0

        time.sleep(0.5)
        print status

        return

    def on_delete(self, status_id, user_id):
        self.delout.write(str(status_id) + "\n")
        return

    def on_limit(self, track):
        sys.stderr.write(track + "\n")
        return

    def on_error(self, status_code):
        sys.stderr.write('Error: ' + str(status_code) + "\n")
        return False

    def on_timeout(self):
        sys.stderr.write("Timeout, sleeping for 60 seconds...\n")
        time.sleep(60)
        return


def write_to_mongo(server, port, db, collection, document):
    client = MongoClient(server, port)
    db = client[db]
    collection = db[collection]
    collection.insert(json.loads(document))
