import os
import sys
import traceback

sys.path.append(os.path.join(os.getcwd(), '..'))

from app.settings import *
from mediawiki.personality import create_page_from_node
from app.entity_iterators import N4JQuery
import mwclient
import py2neo
import json

wiki_site = mwclient.Site(WIKI_SITE, path=WIKI_PATH)
wiki_site.login(WIKI_USER, WIKI_PASSWORD)
py2neo.authenticate(NEO4J_URL, NEO4J_USER, NEO4J_PASSWORD)
graph = py2neo.Graph('http://' + NEO4J_URL + NEO4J_GRAPH)
authorities = graph.cypher.execute('match (n:Person) where exists(n.person_name_heb) return count(n) as count')
#authorities = graph.cypher.execute("match (n:Person) where n.id = '000017959' return n")
# authorities = graph.cypher.execute('match (p:Person)-[]-(r) with p, count(r) as rels where exists(p.person_name_heb) and rels > 0 return p')

total_authorities = authorities.records[0].count
for authority_index, person_node in enumerate(N4JQuery('match (n:Person) where exists(n.person_name_heb) return n')):
        person_id = person_node.n['id']
        print("%s / %s. : %s " % (authority_index, total_authorities, person_id), end="", flush=True)
        try:
            #print(person_node.n['person_name_heb'].encode('utf-8'))
            person_json = json.loads(person_node.n['data'])
            #print(person_json)
            records = graph.cypher.execute('match (p:Person {id:"' + person_id + '"})-[r]-(node) return type(r) as rel_type, node')
            records_list = {'author_of' : {}, 'subject_of' : {}, 'portrait_of' : {}}
            print("%s records" % len(records))
            for record in records:
                record_data = eval(record.node['data'])
                record_control = record_data['control']
                record_display = record_data['display']
                record_type = record_display['type']
                record_links = record_data['links']
                record_notes = record_display.get('lds05',[""])
                if type(record_notes) is str:
                    record_notes = [record_notes]
                record_notes = "<br>".join(record_notes)
                record_fields = {
                    'nnl' : record_control['sourcerecordid'],
                    'nnl_prefix' : record_control['sourceid'],
                    'title' : record.node['title'],
                    'description' : record.node['title'], # TODO: same as title?
                    'date' : record_display.get('creationdate', 'לא ידוע'),
                    'notes' : record_notes,
                    'rosetta' : record_links.get('linktorsrc',''),
                    'fl' : record.node['fl'],
                    'language' : record_display.get('language','heb')
                }
                rel_type = record.rel_type # either author_of or subject_of
                # create_page_from_dictionary(record_dict)
                if record_type not in records_list[rel_type]:
                    records_list[rel_type][record_type] = []
                records_list[rel_type][record_type].append(record_fields)
            create_page_from_node(person_node.n, records_list, site=wiki_site)
        except Exception as e:
            print(e)
            traceback.print_exc(file=sys.stdout)
            # try:
            #     create_page_from_node(person_node, records_list)
            # except Exception as e:
                #     print('boom')
                #     print(e)
