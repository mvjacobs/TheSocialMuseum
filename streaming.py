__author__ = 'marc'

from SListener import SListener
from pymongo import MongoClient
import tweepy


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
    track = get_hashtags()
    #track = ["#nieuweverdeling"]
    follow = []

    listen = SListener(api, 'museum', ou_type='mongo')
    stream = tweepy.Stream(auth, listen)

    print "Streaming started on %s users and %s keywords..." % (len(track), len(follow))

    try:
        stream.filter(track=track, follow=follow)
    except:
        print "error!"
        stream.disconnect()


def get_hashtags():
    client = MongoClient('localhost', 27017)
    db = client.museums
    collection = db.museum_hashtags
    cursor = collection.find()

    hashtags = []
    for document in cursor:
        hashtags.append(document['hashtag'])

    return hashtags

if __name__ == '__main__':
    main()
