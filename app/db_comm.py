import py2neo
import re
import os
import traceback
import sys
import py2neo.cypher

sys.path.append(os.path.join(os.getcwd(), '..'))
from app.entity_iterators import Portraits, Photos, get_authorities, Results, N4JQuery
from app.settings import *
from py2neo.packages.httpstream import http
from py2neo.cypher import MergeNode

http.socket_timeout = 9999

py2neo.authenticate(NEO4J_URL, NEO4J_USER, NEO4J_PASSWORD)
graph = py2neo.Graph('http://' + NEO4J_URL + NEO4J_GRAPH)


def get_entity_node(entity):
    return py2neo.Node(*entity.labels, **entity.properties)
    return graph.merge_one(entity.labels[0], "id", entity.properties["id"])


def create_entity(entity):
    entity_node = get_entity_node(entity)
    entity_node.properties.update(**entity.properties)
    for label in entity.labels:
        entity_node.labels.add(label)
    return entity_node


def set_entities(entities):
    tx = graph.cypher.begin()
    for i, entity in enumerate(entities):
        tx.append(MergeNode(entity.labels[0], "id", entity.properties["id"]).set(*entity.labels, **entity.properties))
        print(i, entity.properties["id"], flush=True)
        if i % 200 == 0:
            tx.commit()
            tx = graph.cypher.begin()


def set_portraits():
    # people = graph.cypher.execute(
    # "match (n:Person) where n.id = '000121498' return n as node")
    for i, person in enumerate(N4JQuery('match (n:Person {id:"000017959"}) where exists(n.person_name_absolute) return n as node', page_size=4, offset=0)):
        try:
            authority_portrait(person)
        except Exception as e:
            print(e, flush=True)
            traceback.print_exc(file=sys.stdout)


def authority_portrait(authority):
    query = authority.node.properties["person_name_absolute"]
    print("Portrait {} : {}".format(authority.node.properties["id"], query), flush=True)
    portraits = Portraits(query)
    for portrait in portraits:
        print(portrait)
        portrait_node = create_entity(portrait)
        graph.create_unique(py2neo.Relationship(authority.node, "subject_of", portrait_node))
        graph.create_unique(py2neo.Relationship(authority.node, "portrait_of", portrait_node))


def set_photos():
    # people = graph.cypher.execute(
    # "match (n:Person) where n.id = '000121498' return n as node")
    for i, person in enumerate(N4JQuery("match (n:Person) where exists(n.person_name_heb) return n as node")):
        try:
            authority_photos(person)
        except Exception as e:
            print(e, flush=True)
            traceback.print_exc(file=sys.stdout)

def authority_photos(authority):
    query = authority.node.properties["person_name_heb"]
    print("Photo {} : {}".format(authority.node.properties["id"], query), flush=True)
    photos = Photos(query)
    for photo, _ in zip(photos, range(10)):
        portrait_node = create_entity(photo)
        graph.create_unique(py2neo.Relationship(authority.node, "subject_of", portrait_node))


def create_records_authorities_relationships():
    for i, record in enumerate(N4JQuery("match (n:Record) return n as node")):
        try:
            data = record.node.properties["data"]
            # data may contain more than one value
            if type(data) is str:
                dat = eval(data)
            else:
                dat = eval(data[0])
            if not data or not dat.get('browse'):
                continue
            print(i, dat.get('control').get('recordid'), end="")
            authors, subjects = authorities_of_record(dat.get('browse'))
            if authors:
                print(" Authors:", authors, end="")
                create_relationship(authors, record.node, 'author_of')
            if subjects:
                print(" Subjects:", subjects, end="")
                create_relationship(subjects, record.node, 'subject_of')
            print(flush=True)
        except Exception as e:
            print(e, flush=True)
            traceback.print_exc(file=sys.stdout)

    graph.push()


def authorities_of_record(authorities):
    if not authorities:
        return None, None
    authors_set = extract_authority('author', authorities)
    subjects_set = extract_authority('subject', authorities)
    return authors_set, subjects_set


def create_relationship(authorities, record, relation):
    if not authorities:
        return
    for authority in authorities:
        node = graph.merge_one("Authority", "id", authority)
        graph.create_unique(py2neo.Relationship(node, relation, record))


def extract_authority(relationship, authorities):
    find_id = re.compile(r"INNL\d{11}\$\$").search
    return authorities.get(relationship) and {find_id(authority).group()[6:-2] for authority in
                                              authorities[relationship] if find_id(authority)}

# set_entities(get_authorities(from_id=0))
# set_entities(Results('NNL_ALEPH'))
# print("Done getting records", flush=True)
# create_records_authorities_relationships()
set_portraits()
set_photos()
graph.match()
print("done", flush=True)
