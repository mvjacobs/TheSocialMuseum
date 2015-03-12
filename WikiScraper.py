__author__ = 'marc'

from lxml import html
from SPARQLWrapper import SPARQLWrapper, JSON
import requests, json, urlparse, urllib2


def main():
    # scrape wikipedia page with a list of all museums in the Netherlands
    entities_nl = get_entities('http://nl.wikipedia.org/wiki/Lijst_van_musea_in_Nederland',
                               '//div[@id="bodyContent"]/div[@id="mw-content-text"]'
                               '/h3/following-sibling::ul/li/a[not(@rel="nofollow")]/@href')
    entities_adam = get_entities('http://nl.wikipedia.org/wiki/Lijst_van_musea_in_Amsterdam',
                                 '//div[@id="bodyContent"]/div[@id="mw-content-text"]'
                                 '/ul[1]/li/a[not(@rel="nofollow")]/@href')
    museums_urls = entities_nl + entities_adam
    preprocess_entities(museums_urls)


def get_wiki_title(wiki_url):
    museum_page = requests.get('http://nl.wikipedia.org' + wiki_url)
    museum_tree = html.fromstring(museum_page.text)
    museum_entity = museum_tree.xpath('//h1[@id="firstHeading"]/text()')

    if museum_entity[0].startswith('Bezig'):
        parsed = urlparse.urlparse(wiki_url)
        return urlparse.parse_qs(parsed.query)['title'][0]

    return museum_entity[0].replace(' ', '_')


def check_blacklist(wiki_title):
    blacklist = [line.strip() for line in open('config/blacklist.cfg')]
    return wiki_title.startswith(tuple(blacklist))


def get_entities(wiki_page, xpath):
    # scrape wikipedia page with a list of all museums in the Netherlands
    page = requests.get(wiki_page)
    tree = html.fromstring(page.text)
    return tree.xpath(xpath)


def get_geo_data(museum_title):
    # get geo information for museum
    geo_url = "https://maps.googleapis.com/maps/api/geocode/json?address=%s" % museum_title
    geo_response = urllib2.urlopen(geo_url)
    geo_jsongeocode = geo_response.read()
    return json.loads(geo_jsongeocode)


def get_dbpedia_data(museum_title):
    dbpedia_page = 'http://nl.dbpedia.org/resource/' + museum_title

    # Get dbpedia properties for museum
    sparql = SPARQLWrapper("http://nl.dbpedia.org/sparql")
    query = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " \
            "SELECT * WHERE { <%s> ?property ?entity }" % dbpedia_page
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    return results["results"]["bindings"]


def get_hashtag(museum_title):
    return '#%s' % museum_title.replace('_', '')


def remove_duplicates(dictionary):
    result = {}
    for key, value in dictionary.items():
        if value not in result.values():
            result[key] = value

    return result


def create_collections(collection_name, collection):
    with open('data/%s.json' % collection_name, 'w') as outfile:
        json.dump(remove_duplicates(collection), outfile)


def preprocess_entities(museums_urls):
    museums = []
    hashtags = []
    locations = []
    succeeded = 0
    failed = 0

    for url in museums_urls:
        try:
            museum_title = get_wiki_title(url)

            if check_blacklist(museum_title): continue
            geo_data = get_geo_data(museum_title)
            if geo_data['status'] == "ZERO_RESULTS": continue

            # Add the results to the right collection
            museums.append({'id': museum_title, 'dbpedia': get_dbpedia_data(museum_title)})
            hashtags.append({'id': museum_title, 'hashtag': get_hashtag(museum_title)})
            locations.append({'id': museum_title, 'location': geo_data['results'][0]})

            succeeded += 1
            print "processed entity: %s" % museum_title
        except:
            failed += 1
            pass

    create_collections('museums', museums)
    create_collections('hashtags', hashtags)
    create_collections('locations', locations)

    print "Total %s entities, %s succeeded, %s failed." % (failed+succeeded, succeeded, failed)


if __name__ == '__main__':
    main()