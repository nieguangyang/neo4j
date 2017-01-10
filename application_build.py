#!/usr/bin/env python
# coding=utf8
from py2neo import *

g = Graph(host="localhost", http_port=7474, user="neo4j", password="multisim2011")
# overdue
# phonenumbers
statement1 = """
load csv with headers from "file:///application_t.csv" AS row
create (a:application)
set
  a.aid = row.aid,
  a.uid = row.uid,
  a.pid = row.pid,
  a.apply_time = row.apply_time,
  a.apply_timestamp = toInt(row.apply_timestamp)
"""
# calllog internal
statement2 = """
using periodic commit 10000
load csv with headers from "file:///application_calllog_t.csv" AS row
match
  (a:application {pid: row.caller}),
  (p:phonenumber:overdue {pid: row.callee})
create (a)-[r:calllog]->(p)
set
  r.cnt = toInt(row.cnt),
  r.duration = toInt(row.duration),
  r.last_contact_time = row.last_contact_time,
  r.last_contact_timestamp = toInt(row.last_contact_timestamp)
"""
# phonebook internal
statement3 = """
using periodic commit 10000
load csv with headers from "file:///application_phonebook_t.csv" AS row
match
  (a:application {pid: row.caller}),
  (p:phonenumber:overdue {pid: row.callee})
create (p1)-[r:phonebook]->(p2)
set
  r.last_contact_time = row.last_contact_time,
  r.last_contact_timestamp = toInt(row.last_contact_timestamp)
"""

print "----------> nodes: application"
g.run(statement1)
print "----------> indexing"
g.run("create index on :application(pid)")
print "----------> relationships: calllog"
g.run(statement2)
print "----------> relationships: phonebook"
g.run(statement3)









