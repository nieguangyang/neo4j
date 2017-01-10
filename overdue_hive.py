#!/usr/bin/env python
# coding=utf8

# hive

min_date = '2016-04-01'
max_date = '2016-09-30'
current_date = '2017-01-05'

print "----------> overdue loan"
hql1 = r"""
hive -e "
use dante;

drop table overdue_loan;

create table overdue_loan as
select lid,
    uid,
    mobile as pid,
    apply_time,
    apply_timestamp,
    disburse_time,
    disburse_timestamp,
    installments,
    payable_installments,
    overdue_installments,
    amount,
    overdue_amount
from
(
    select aid,
        lid,
        uid,
        apply_time,
        unix_timestamp(apply_time) as apply_timestamp,
        disburse_time,
        unix_timestamp(disburse_time) as disburse_timestamp,
        installments,
        if(payable_installments > installments, installments, payable_installments) as payable_installments,
        overdue_installments,
        amount,
        overdue_amount
    from
    (
        select id as aid,
            substr(applied_at, 0, 19) as apply_time
        from source_data.loan_applications
        where (to_date(applied_at) >= '%(min_date)s' and to_date(applied_at) <= '%(max_date)s')
    ) t1
    join
    (
        select loan_application_id,
            id as lid,
            borrower_id as uid,
            substr(disbursed_at, 0, 19) as disburse_time,
            months as installments,
            cast(datediff('%(current_date)s', to_date(disbursed_at))/30 as int) as payable_installments,
            overdue_months as overdue_installments,
            amount,
            overdue_amt_tot as overdue_amount
        from source_data.loans
        where overdue_months > 0
        and to_date(disbursed_at) >= '%(min_date)s'
    ) t2
    on t1.aid = t2.loan_application_id
) t3
join source_data.users_basic t4
on t3.uid = t4.id
order by apply_time;
"
""" % {'min_date': min_date, 'max_date': max_date, 'current_date': current_date}
print hql1

print "----------> overdue phonenumber"
hql2 = r"""
hive -e "
use dante;

drop table overdue_phonenumber;

create table overdue_phonenumber as
select pid,
    count(*) as overdue_loans,
    sum(installments) as installments,
    sum(payable_installments) as payable_installments,
    sum(overdue_installments) as overdue_installments,
    sum(amount) as amount,
    sum(overdue_amount) as overdue_amount,
    max(apply_time) as last_apply_time,
    max(apply_timestamp) as last_apply_timestamp
from dante.overdue_loan
group by pid;
"
"""
print hql2

print "----------> overdue calllog"
hql3 = r"""
hive -e "
use dante;

drop table overdue_calllog;

create table overdue_calllog as
select caller,
    callee,
    cnt,
    duration,
    last_contact_time,
    unix_timestamp(last_contact_time) as last_contact_timestamp,
    if(t4.pid is not null, 1, 0) as internal
from
(
    select account as caller,
        phonenumber as callee,
        count(*) as cnt,
        sum(lduration) as duration,
        from_unixtime(floor(max(ldate)/1000)) as last_contact_time
    from source_data.calllog_deduplication_parquet t1
    join dante.overdue_phonenumber t2
    on t1.account = t2.pid
    where phonenumber rlike '^1(3[0-9]|4[57]|5[0-35-9]|7[01678]|8[0-9])\\\d{8}$'
    and lduration >= 5
    and account != phonenumber
    and itype in (1, 2)
    group by account, phonenumber
) t3
left join dante.overdue_phonenumber t4
on t3.callee = t4.pid;
"
"""
print hql3

print "----------> overdue phonebook"
hql4 = r"""
hive -e "
use dante;

drop table overdue_phonebook;

create table overdue_phonebook as
select caller,
    callee,
    last_contact_time,
    unix_timestamp(last_contact_time) as last_contact_timestamp,
    if(t7.pid is not null, 1, 0) as internal
from
(
    select caller,
        callee,
        max(last_contact_time) as last_contact_time
    from
    (
        select account as caller,
            phonenumber as callee,
            from_unixtime(floor(lasttimecontact/1000)) as last_contact_time
        from source_data.phonebook_deduplication_parquet t1
        join dante.overdue_phonenumber t2
        on t1.account = t2.pid
        where phonenumber rlike '^1(3[0-9]|4[57]|5[0-35-9]|7[01678]|8[0-9])\\\d{8}$'
        and account != phonenumber
        union all
        select account as caller,
            phonenumber as callee,
            substr(lastrecordtime, 0, 19) as last_contact_time
        from source_data.phonebookios_deduplication_parquet t3
        join dante.overdue_phonenumber t4
        on t3.account = t4.pid
        where phonenumber rlike '^1(3[0-9]|4[57]|5[0-35-9]|7[01678]|8[0-9])\\\d{8}$'
        and account != phonenumber
    ) t5
    group by caller, callee
) t6
left join dante.overdue_phonenumber t7
on t6.callee = t7.pid;
"
"""
print hql4

print "----------> overdue phonenumber supplementary"
hql5 = r"""
hive -e "
use dante;

drop table overdue_phonenumber_supplementary;

create table overdue_phonenumber_supplementary as
select callee,
    count(distinct caller) as caller_cnt
from
(
    select caller,
        callee
    from dante.overdue_calllog
    where internal = 0
    union all
    select caller,
        callee
    from dante.overdue_phonebook
    where internal = 0
) t
group by callee
order by caller_cnt desc;
"
"""
print hql5







