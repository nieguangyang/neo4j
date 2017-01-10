#!/usr/bin/env python
# coding=utf8
from py2neo import *

g = Graph(host="localhost", http_port=7474, user="neo4j", password="multisim2011")

# overdue phonenumber
ovd_phn = """
using periodic commit 10000
load csv with headers from "file:///t_overdue_phonenumber.csv" AS row
create (p:phonenumber:overdue)
set
  p.pid = row.pid,
  p.overdue_installments = toInt(row.overdue_installments),
  p.payable_installments = toInt(row.payable_installments),
  p.installments = toInt(row.installments),
  p.amount = toFloat(row.amount),
  p.overdue_amount = toFloat(row.overdue_amount),
  p.overdue_loans = toInt(row.overdue_loans),
  p.last_apply_time = row.last_apply_time,
  p.last_apply_timestamp = toInt(row.last_apply_timestamp)
"""

# overdue phonenumber supplementary
ovd_phn_sup = """
using periodic commit 10000
load csv with headers from "file:///t_overdue_phonenumber_supplementary.csv" AS row
create (p:phonenumber:supplementary)
set
  p.pid = row.callee,
  p.cnt = toInt(row.caller_cnt)
"""
'''
print "----------> create node :phonenumber:overdue"
g.run(ovd_phn)
print "----------> create node :phonenumber:supplementary"
g.run(ovd_phn_sup)
print "----------> index node :phonenumber on pid"
g.run("create index on :phonenumber(pid)")
print "----------> index node :overdue on pid"
g.run("create index on :overdue(pid)")
print "----------> index node :supplementary on pid"
g.run("create index on :supplementary(pid)")
'''

# overdue calllog internal
ovd_cal_int = """
using periodic commit 10000
load csv with headers from "file:///t_overdue_calllog_internal.csv" AS row
match
  (p1:phonenumber:overdue {pid: row.caller}),
  (p2:phonenumber:overdue {pid: row.callee})
create (p1)-[r:calllog]->(p2)
set
  r.cnt = toInt(row.cnt),
  r.duration = toInt(row.duration),
  r.last_contact_time = row.last_contact_time,
  r.last_contact_timestamp = toInt(row.last_contact_timestamp)
"""

# overdue phonebook internal
ovd_phb_int = """
using periodic commit 10000
load csv with headers from "file:///t_overdue_phonebook_internal.csv" AS row
match
  (p1:phonenumber:overdue {pid: row.caller}),
  (p2:phonenumber:overdue {pid: row.callee})
create (p1)-[r:phonebook]->(p2)
set
  r.last_contact_time = row.last_contact_time,
  r.last_contact_timestamp = toInt(row.last_contact_timestamp)
"""
'''
print "----------> create relationships :calllog internal"
g.run(ovd_cal_int)
print "----------> create relationships :phonebook internal"
g.run(ovd_phb_int)
'''

# overdue calllog external
ovd_cal_ext = """
using periodic commit 10000
load csv with headers from "file:///t_overdue_calllog_external.csv" AS row
match
  (p1:phonenumber:overdue {pid: row.caller}),
  (p2:phonenumber:supplementary {pid: row.callee})
create (p1)-[r:calllog]->(p2)
set
  r.cnt = toInt(row.cnt),
  r.duration = toInt(row.duration),
  r.last_contact_time = row.last_contact_time,
  r.last_contact_timestamp = toInt(row.last_contact_timestamp)
"""

# overdue phonebook external
ovd_phb_ext = """
using periodic commit 10000
load csv with headers from "file:///t_overdue_phonebook_external.csv" AS row
match
  (p1:phonenumber:overdue {pid: row.caller}),
  (p2:phonenumber:supplementary {pid: row.callee})
create (p1)-[r:phonebook]->(p2)
set
  r.last_contact_time = row.last_contact_time,
  r.last_contact_timestamp = toInt(row.last_contact_timestamp)
"""

print "----------> create relationships :calllog external"
g.run(ovd_cal_ext)
print "----------> create relationships :phonebook external"
g.run(ovd_phb_ext)
