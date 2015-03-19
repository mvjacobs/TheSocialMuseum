__author__ = 'Nicky'

from pymongo import MongoClient
import json

client = MongoClient('localhost', 27017)
db = client.museums
out_file = open("data/related_museums.js","w")
data = []
allHashtags = []

for museum in db.museum_locations.find():
    collectedHashtags = []
    relatedMuseums = []
    name = museum['id']

    for hashtag in db.museum_hashtags.find():
        allHashtags.append(hashtag['hashtag'].replace("#", ""))

    for hashtag in db.museum_hashtags.find():
        hashtagString = hashtag['hashtag'].replace("#", "")
        if hashtag['id'] == name:
            for tweets in db.museum_tweets.find():
                hashtagsAmount = len(tweets['entities']['hashtags'])
                for index in range(hashtagsAmount):
                    if tweets['entities']['hashtags'][index]['text'] == hashtagString:
                        for index2 in range(hashtagsAmount):
                            if tweets['entities']['hashtags'][index2]['text'] != hashtagString:
                                if tweets['entities']['hashtags'][index2]['text'] not in collectedHashtags:
                                    collectedHashtags.append(tweets['entities']['hashtags'][index2]['text'])

    for index3 in range(len(collectedHashtags)):
        if collectedHashtags[index3] in allHashtags:
            relatedMuseums.append(collectedHashtags[index3])

    if len(relatedMuseums) >= 1:
        for index4 in range(len(relatedMuseums)):
            row = {'Museum': name, 'Related_Museum': relatedMuseums[index4]}
            data.append(row)

json.dump(data, out_file)
print data
