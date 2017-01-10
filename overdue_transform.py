#!/usr/bin/env python
# coding=utf8

# transform

print "----------> transform overdue phonenumber"
fin = open("overdue_phonenumber.csv", "r")
fout = open("t_overdue_phonenumber.csv", "w")
fout.write("pid,overdue_loans,installments,payable_installments,overdue_installments,amount,overdue_amount," +
           "last_apply_time,last_apply_timestamp\n")
for line in fin:
    parameters = line.strip("\n").split("\t")
    fout.write(",".join(parameters[:9]) + "\n")
fin.close()
fout.close()

print "----------> transform overdue calllog internal"
fin = open("overdue_calllog_internal.csv", "r")
fout = open("t_overdue_calllog_internal.csv", "w")
fout.write("caller,callee,cnt,duration,last_contact_time,last_contact_timestamp\n")
for line in fin:
    parameters = line.strip("\n").split("\t")
    fout.write(",".join(parameters[:6]) + "\n")
fin.close()
fout.close()

print "----------> transform overdue phonebook internal"
fin = open("overdue_phonebook_internal.csv", "r")
fout = open("t_overdue_phonebook_internal.csv", "w")
fout.write("caller,callee,last_contact_time,last_contact_timestamp\n")
for line in fin:
    parameters = line.strip("\n").split("\t")
    fout.write(",".join(parameters[:4]) + "\n")
fin.close()
fout.close()

print "----------> transform overdue phonenumber supplementary"
fin = open("overdue_phonenumber_supplementary.csv", "r")
fout = open("t_overdue_phonenumber_supplementary.csv", "w")
fout.write("callee,caller_cnt\n")
for line in fin:
    parameters = line.strip("\n").split("\t")
    fout.write(",".join(parameters[:2]) + "\n")
fin.close()
fout.close()

print "----------> transform overdue calllog external"
fin = open("overdue_calllog_external.csv", "r")
fout = open("t_overdue_calllog_external.csv", "w")
fout.write("caller,callee,cnt,duration,last_contact_time,last_contact_timestamp\n")
for line in fin:
    parameters = line.strip("\n").split("\t")
    fout.write(",".join(parameters[:6]) + "\n")
fin.close()
fout.close()

print "----------> transform overdue phonebook external"
fin = open("overdue_phonebook_external.csv", "r")
fout = open("t_overdue_phonebook_external.csv", "w")
fout.write("caller,callee,last_contact_time,last_contact_timestamp\n")
for line in fin:
    parameters = line.strip("\n").split("\t")
    fout.write(",".join(parameters[:4]) + "\n")
fin.close()
fout.close()









