import os
import sys

sys.path.append(os.path.join(os.getcwd(), '..'))

from app.settings import *
from app.entity_iterators import N4JQuery
import py2neo
import json

subdivisions = ["form", "geographical", "general", "chronological"]

def keys(subdivisions, separator = "|"):
    return separator.join(subdivisions.values())


# iterate over all primary locations with hebrew name
# TODO: not only Locations
for authority_index, authority_node in enumerate(N4JQuery('match (n:Location {primary:true}) where exists(n.location_name_heb) return n as node, labels(n) as labels')):
    primary_id = authority_node.node['id']
    node_type = authority_node.labels[-1] # first label is Authority, second (last) is the actual type (Location/Person/...)
    if (node_type == "Authority"):
        continue
    name_heb = "{}_name_{}".format(node_type.lower(), "heb")
    authority_name_heb = authority_node.node[name_heb]
    if authority_name_heb:
        # Look for authorities with the same name and primary = false
        authority_name_heb = authority_name_heb.replace('"', '\\"')
        print("==================")
        print("%s : %s " % (authority_index, primary_id))
        print(authority_node.node, flush=True)
        query = 'match (n:{authority} {{primary:false, {key}:"{val}"}}) return n'.format(authority=node_type, key=name_heb, val=authority_name_heb)
        sub_sub = {} # map from keys(subdivisions) to non-primary authority id
        reverse_subs = {} # map from non-primary authority id to its subdivisions
        for sub_index, sub_node in enumerate(N4JQuery(query, page_size=1000)):
            sub_id = sub_node.n['id']
            authority_json = json.loads(sub_node.n['data'])
            print("    ", sub_id, authority_json)
            subdivisions_dict = {}
            # extract subdivision values
            for sub_type in subdivisions:
                sub_type_name = "{}_subdivision_{}".format(sub_type, "heb")
                sub_type_val = sub_node.n[sub_type_name]
                subdivisions_dict[sub_type_name] = sub_type_val if sub_type_val else ""
            print("        ", subdivisions_dict)
            key = keys(subdivisions_dict)
            sub_sub[key] = sub_id
            reverse_subs[sub_id] = subdivisions_dict

        # Go over all non-primary authorities and find the parents of each
        for key in sub_sub:
            id = sub_sub[key]
            subdivisions_dict = reverse_subs[id]
            for sub_type_name in subdivisions_dict:
                if subdivisions_dict[sub_type_name]:
                    subs_copy = dict(subdivisions_dict)
                    subs_copy[sub_type_name] = ""
                    parent_key = keys(subs_copy)
                    parent_id = primary_id if parent_key == "|||" else sub_sub.get(parent_key)
                    print("{} is parent of {}".format(parent_id, id))

