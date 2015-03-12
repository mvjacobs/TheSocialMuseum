__author__ = 'marc'

from SListener import SListener
from pymongo import MongoClient
import tweepy, sys


lines = [line.strip() for line in open('config/twitter.cfg')]
consumer_key = lines[0]
consumer_secret = lines[1]
access_token = lines[2]
access_token_secret = lines[3]

# OAuth process, using the keys and tokens
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)


def main(mode=1):
    if len(sys.argv) != 3:
        print "please provide an offset and a limit (e.g. python streaming.py 0 350)"
        exit()

    track = get_hashtags(int(sys.argv[1]), int(sys.argv[2]))
    follow = []

    listen = SListener(api, 'museum', ou_type='mongo')
    stream = tweepy.Stream(auth, listen)

    print "Streaming started on %s users and %s keywords..." % (len(track), len(follow))

    try:
        stream.filter(track=track, follow=follow)
    except:
        print "error!"
        stream.disconnect()


def get_hashtags(offset, limit):
    client = MongoClient('localhost', 27017)
    db = client.museums
    collection = db.museum_hashtags

    cursor = collection.find().skip(offset).limit(limit)

    hashtags = []
    for document in cursor:
        hashtags.append(document['hashtag'])

    return hashtags

if __name__ == '__main__':
    main()
