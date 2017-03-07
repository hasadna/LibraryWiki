import os
import sys
import traceback

sys.path.append(os.path.join(os.getcwd(), '..'))

from mediawiki.pages import create_page_from_dictionary
import py2neo
import mwclient
from app.settings import *

py2neo.authenticate(NEO4J_URL, NEO4J_USER, NEO4J_PASSWORD)
graph = py2neo.Graph('http://' + NEO4J_URL + NEO4J_GRAPH)

PAGE_SIZE = 20
page_number = 0
record_count = 0
wiki_site = mwclient.Site(WIKI_SITE, path=WIKI_PATH)
wiki_site.login(WIKI_USER, WIKI_PASSWORD)
while True:
    skip = PAGE_SIZE * page_number
    records = graph.cypher.execute('match (p:Person)-[]-(r) with p, r, count(r) as rels where exists(p.person_name_heb) and rels > 0 return r skip {} limit {}'.format(skip, PAGE_SIZE))
    # records = graph.cypher.execute('match (p:Person)-[]-(r) with p, r, count(r) as rels where rels > 0 return r skip {} limit {}'.format(skip, PAGE_SIZE))
    # records = graph.cypher.execute('match (r:Person) return r skip {} limit {}'.format(skip, PAGE_SIZE))
    # records = graph.cypher.execute('match (r:Record {id:"NNL_ALEPH001975996"}) return r')
    if not len(records):
        break
    page_number += 1
    for nodes in records:
        for record in nodes:
            record_count += 1
            record_data = eval(record['data'])
            if record_data.get('control'):
                print(str("{}. {}".format(record_count, record_data['control']['recordid'])), end="\n", flush=True)
                try:
                    create_page_from_dictionary(record_data, site=wiki_site)
                except Exception as e:
                    print(e)
                    traceback.print_exc(file=sys.stdout)

print("Done. %s records processed" % record_count)
