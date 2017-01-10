#!/usr/bin/env python
# coding=utf8
from py2neo import *

g = Graph(host="localhost", http_port=7474, user="neo4j", password="multisim2011")
n = g.find_one("phonenumber", "pid", 13972229841)
'''
print dict(n)
print "n->"
for r in g.match(start_node=n):
    print r.end_node()
print "->n"
for r in g.match(end_node=n):
    print r.start_node()
print "U"
related_nodes = set([r.end_node() for r in g.match(start_node=n)] + [r.start_node() for r in g.match(end_node=n)])
for node in related_nodes:
    print node
'''

def breath_first_search(start_node, property_key, max_depth=7):
    # queue of (node, path)
    queue = [(start_node, [])]
    passed = [start_node[property_key]]
    i = 0
    while len(queue) > 0:
        node, path = queue.pop(0)
        _path = [x for x in path]
        _path.append(node[property_key])
        i += 1
        print "->".join([str(x) for x in _path]), i
        if len(_path) == max_depth:
            continue
        for _node in set([r.end_node() for r in g.match(start_node=node)] + [r.start_node() for r in g.match(end_node=node)]):
            if _node[property_key] in passed:
                continue
            queue.append((_node, _path))
            passed.append(_node[property_key])

def connected_component():
    pass

breath_first_search(n, "pid")


