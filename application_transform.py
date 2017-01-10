#!/usr/bin/env python
# coding=utf8

# transform

print "----------> transform application"
fin = open("application.csv", "r")
fout = open("application_t.csv", "w")
fout.write("aid,uid,pid,apply_time,apply_timestamp\n")
for line in fin:
    parameters = line.strip("\n").split("\t")
    fout.write(",".join(parameters[:5]) + "\n")
fin.close()
fout.close()

print "----------> transform application calllog"
fin = open("application_calllog.csv", "r")
fout = open("application_calllog_t.csv", "w")
fout.write("caller,callee,cnt,duration,last_contact_time,last_contact_timestamp\n")
for line in fin:
    parameters = line.strip("\n").split("\t")
    fout.write(",".join(parameters[:6]) + "\n")
fin.close()
fout.close()

print "----------> transform application phonebook"
fin = open("application_phonebook.csv", "r")
fout = open("application_phonebook_t.csv", "w")
fout.write("caller,callee,last_contact_time,last_contact_timestamp\n")
for line in fin:
    parameters = line.strip("\n").split("\t")
    fout.write(",".join(parameters[:4]) + "\n")
fin.close()
fout.close()









