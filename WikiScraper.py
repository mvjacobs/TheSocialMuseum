__author__ = 'marc'

from lxml import html
from SPARQLWrapper import SPARQLWrapper, JSON
import requests, json, urlparse, urllib2


def main():
    """Main function
    The script starts here.
    Scrape wikipedia page with a list of all museums in the Netherlands
    Prerequisites:
    - create a "/data" folder
    - ensure a stable internet connection
    """
    entities_nl = get_entities('http://nl.wikipedia.org/wiki/Lijst_van_musea_in_Nederland',
                               '//div[@id="bodyContent"]/div[@id="mw-content-text"]'
                               '/h3/following-sibling::ul/li/a[not(@rel="nofollow")]/@href')
    entities_adam = get_entities('http://nl.wikipedia.org/wiki/Lijst_van_musea_in_Amsterdam',
                                 '//div[@id="bodyContent"]/div[@id="mw-content-text"]'
                                 '/ul[1]/li/a[not(@rel="nofollow")]/@href')
    museums_urls = entities_adam + entities_nl
    preprocess_entities(museums_urls)


def get_wiki_title(wiki_url):
    """Get Wiki Title
    Tries to access the wikipedia page of a given url
    If the page does not yet exits, return the title element from the URL
    If the page exists, return the header H1 name
    """
    museum_page = requests.get('http://nl.wikipedia.org' + wiki_url)
    museum_tree = html.fromstring(museum_page.text)
    museum_entity = museum_tree.xpath('//h1[@id="firstHeading"]/text()')

    if museum_entity[0].startswith('Bezig'):
        parsed = urlparse.urlparse(wiki_url)
        return urlparse.parse_qs(parsed.query)['title'][0]

    return museum_entity[0].replace(' ', '_')


def check_blacklist(wiki_title):
    """Check blacklist
    Returns True if the given title exists in the blacklist
    """
    blacklist = [line.strip() for line in open('config/blacklist.cfg')]
    return wiki_title in blacklist


def get_entities(wiki_page, xpath):
    """Get wikipedia entities
    Scrape HTML page using an URL and xpath query
    """
    page = requests.get(wiki_page)
    tree = html.fromstring(page.text)
    return tree.xpath(xpath)


def get_geo_data(museum_title):
    """Get geodata
    Get geo information for a museum
    Uses the google maps API - https://developers.google.com/maps/
    Beware of the daily request limit
    """
    geo_url = "https://maps.googleapis.com/maps/api/geocode/json?address=%s" % museum_title
    geo_response = urllib2.urlopen(geo_url)
    geo_jsongeocode = geo_response.read()
    return json.loads(geo_jsongeocode)


def get_dbpedia_data(museum_title):
    """Get additional dbpedia data
    Use the dutch Dbpedia to get additional information about a museum
    Uses SPARQL with the DBPedia endpoint to query for museums
    """
    dbpedia_page = 'http://nl.dbpedia.org/resource/' + museum_title
    sparql = SPARQLWrapper("http://nl.dbpedia.org/sparql")
    query = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " \
            "SELECT * WHERE { <%s> ?property ?entity }" % dbpedia_page
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    return results["results"]["bindings"]


def get_hashtag(museum_title):
    """Get hashtag
    GTransform a wikipedia museum entity into a hashtag
    """
    return '#%s' % museum_title.replace('_', '')


def create_collection(collection_name, collection):
    """Create json collection
    Create a json file from a given json array
    """
    with open('data/%s.json' % collection_name, 'w') as outfile:
        json.dump(collection, outfile)


def preprocess_entities(museums_urls):
    """Preprocess entities
    Prepocesses the entities from a given set of wikipedia URL's
    Combines the function above to:
    - find entities
    - add geodata to entities
    - add dbpedia information to entities
    - filter duplicates
    - filter blacklisted entities
    - filter entities without geodata
    Eventually, the enriched entities are processed into json files
    and are put into the /data directory
    """
    museum_titles = []
    museums = []
    hashtags = []
    locations = []
    succeeded = 0
    failed = 0
    duplicated = 0
    blacklisted = 0
    nogeodata = 0

    for url in museums_urls:
        try:
            museum_title = get_wiki_title(url)
            if museum_title in museum_titles:
                duplicated += 1
                print "[duplicated] %s" % museum_title
                continue
            if check_blacklist(museum_title):
                blacklisted += 1
                print "[blacklisted] %s" % museum_title
                continue
            geo_data = get_geo_data(museum_title)
            if geo_data['status'] != "OK":
                nogeodata += 1
                print "[no geodata] %s" % museum_title
                continue

            # Add the results to the right collection
            museums.append({'id': museum_title, 'dbpedia': get_dbpedia_data(museum_title)})
            hashtags.append({'id': museum_title, 'hashtag': get_hashtag(museum_title)})
            locations.append({'id': museum_title, 'location': geo_data['results'][0]})
            museum_titles.append(museum_title)

            succeeded += 1
            print "[success] %s" % museum_title
        except:
            failed += 1
            pass

    create_collection('museums', museums)
    create_collection('hashtags', hashtags)
    create_collection('locations', locations)

    print "Total %s entities, %s succeeded, %s failed, %s duplicated, %s blacklisted, %s no geodata." \
          % (len(museums_urls), succeeded, failed, duplicated, blacklisted, nogeodata)


if __name__ == '__main__':
    main()