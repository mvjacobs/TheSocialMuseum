__author__ = 'marc'

from lxml import html
import requests
from SPARQLWrapper import SPARQLWrapper, JSON
import json
import urllib2

# scrape wikipedia page with a list of all museums in the Netherlands
page = requests.get('http://nl.wikipedia.org/wiki/Lijst_van_musea_in_Nederland')
tree = html.fromstring(page.text)
museums_urls = tree.xpath('//div[@id="bodyContent"]/div[@id="mw-content-text"]'
                          '/h3/following-sibling::ul/li/a[not(@rel="nofollow")]/@href')

museums = []
hashtags = []
locations = []
succeeded = 0
failed = 0

for url in museums_urls:
    try:
        museum_page = requests.get('http://nl.wikipedia.org' + url)
        museum_tree = html.fromstring(museum_page.text)
        museum_entity = museum_tree.xpath('//h1[@id="firstHeading"]/text()')
        museum_title = museum_entity[0].replace(' ', '_')

        blacklist = [line.strip().lower() for line in open('config/blacklist.cfg')]

        if not(museum_title.lower() in blacklist) and not(museum_title.startswith('Bezig')):
            dbpedia_page = 'http://nl.dbpedia.org/resource/' + museum_title

            # get geo information for museum
            geo_url = "https://maps.googleapis.com/maps/api/geocode/json?address=%s" % museum_title
            geo_response = urllib2.urlopen(geo_url)
            geo_jsongeocode = geo_response.read()
            geo_data = json.loads(geo_jsongeocode)
            if geo_data['status'] == "ZERO_RESULTS":
                continue

            # Get dbpedia properties for museum
            sparql = SPARQLWrapper("http://nl.dbpedia.org/sparql")
            query = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " \
                    "SELECT * WHERE { <%s> ?property ?entity }" % dbpedia_page
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()

            # Add the results to the right collection
            museums.append({'id': museum_title, 'dbpedia': results["results"]["bindings"]})
            hashtags.append({'id': museum_title, 'hashtag': '#%s' % museum_title.replace('_', '')})
            locations.append({'id': museum_title, 'location': geo_data["results"][0]})

            succeeded += 1
            print "processed entity: %s" % museum_title
    except:
        failed += 1
        pass

with open('data/museums.json', 'w') as outfile:
    json.dump(museums, outfile)

with open('data/hashtags.json', 'w') as outfile:
    json.dump(hashtags, outfile)

with open('data/locations.json', 'w') as outfile:
    json.dump(locations, outfile)


print "Total %s entities, %s succeeded, %s failed." % (failed+succeeded, succeeded, failed)