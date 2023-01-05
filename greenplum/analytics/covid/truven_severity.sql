

------------------------------------
-- all confirmed dx from inpatient table
------------------------------------
 ---normalize inpatient dx to join dx for COVID confirmed

drop table if exists dev.normalized_dx_inp;


select distinct enrolid,
                msclmid,
                caseid,
                unnest(array[a.pdx, a.dx1, a.dx2, a.dx3, a.dx4]) as dx,
                svcdate 
into dev.normalized_dx_inp
from truven.ccaes a
where svcdate between '2020-01-01' and '2020-12-31';

select count(distinct enrolid) from  dev.truven_covid_inp_dx;
--41358

----------------------
----get confirmed covid in inpatient service table
----------------------

drop table if exists dev.truven_covid_inp_dx;


select distinct a.enrolid,
                a.msclmid,
                a.caseid 
                into dev.truven_covid_inp_dx
from dev.normalized_dx_inp a
join dev.normalized_dx_inp b 
					on a.enrolid = b.enrolid
					and a.msclmid = b.msclmid
where a.dx = 'U071'
   or a.dx = 'U072'
   or a.dx = 'U10'
   or ((a.dx in ('J1282',
                 'J208',
                 'J988')
        and b.dx = 'B9729')
       or (a.dx like 'J22%'
           and b.dx = 'B9729')
       or (a.dx like 'J40%'
           and b.dx = 'B9729')
       or (a.dx like 'J80%'
           and b.dx = 'B9729')
       and a.svcdate between '2020-01-01' and '2020-04-01'
       and b.svcdate between '2020-01-01' and '2020-04-01');


------------------------------------
-- all confirmed dx from outatient table
------------------------------------
 ---normalize outatient dx

drop table if exists dev.normalized_dx_out;


select distinct enrolid,
                msclmid,
                unnest(array[a.dx1, a.dx2, a.dx3, a.dx4]) as dx,
                svcdate 
into dev.normalized_dx_out
from truven.ccaeo a
where svcdate between '2020-01-01' and '2020-12-31';

----------------------
----get confirmed covid in outatient service table
----------------------

drop table if exists dev.truven_covid_out_dx;


select distinct a.enrolid,
                a.msclmid 
into dev.truven_covid_out_dx
from dev.normalized_dx_out a
join dev.normalized_dx_out b on a.enrolid = b.enrolid
and a.msclmid = b.msclmid
where a.dx = 'U071'
   or a.dx = 'U072'
   or a.dx = 'U10'
   or ((a.dx in ('J1282',
                 'J208',
                 'J988')
        and b.dx = 'B9729')
       or (a.dx like 'J22%'
           and b.dx = 'B9729')
       or (a.dx like 'J40%'
           and b.dx = 'B9729')
       or (a.dx like 'J80%'
           and b.dx = 'B9729')
       and a.svcdate between '2020-01-01' and '2020-04-01'
       and b.svcdate between '2020-01-01' and '2020-04-01');



-----------------------------------
------------LVL 1 - Suspected
-------------------------------------

drop table if exists dev.truven_covid_lvl1;


select distinct enrolid into dev.truven_covid_lvl1
from dev.normalized_dx_out
where (dx = 'Z8616'
       or dx = 'U08'
       or dx = 'U09'
       or dx = 'B948');

-----------------------------------
------------LVL 2
-------------------------------------

drop table if exists dev.truven_covid_lvl2;


select distinct a.enrolid,
                a.msclmid into dev.truven_covid_lvl2
from dev.truven_covid_out_dx a
where not exists
      (select 1
       from dev.normalized_dx_out c
       where a.enrolid = c.enrolid
          and a.msclmid = c.msclmid
          and (dx = 'J1289'
               or dx = 'J40'
               or dx = 'J22'
               or dx = 'J988'
               or dx = 'J80'
               or dx = 'R05'
               or dx = 'R0602'
               or dx = 'R0603'
               or dx = 'R509') ) ;

-----------------------------------
------------LVL 3
-------------------------------------

drop table if exists dev.truven_covid_lvl3;

select distinct a.enrolid 
into dev.truven_covid_lvl3
from dev.truven_covid_out_dx a
where exists
      (select 1
       from dev.normalized_dx_out c
       where a.enrolid = c.enrolid
          and a.msclmid = c.msclmid
          and (dx = 'J1289'
               or dx = 'J40'
               or dx = 'J22'
               or dx = 'J988'
               or dx = 'J80'
               or dx = 'R05'
               or dx = 'R0602'
               or dx = 'R0603'
               or dx = 'R509') ) ;

-----------------------------------
------------LVL 4
-------------------------------------

drop table if exists dev.truven_covid_lvl4;


select distinct a.enrolid 
into dev.truven_covid_lvl4
from dev.truven_covid_out_dx a
join truven.ccaeo b 
			on a.enrolid = b.enrolid
			and a.msclmid = b.msclmid
where b.revcode between '0450' and '0459'
   and not exists
      (select 1
       from truven.ccaei c
       where a.enrolid = c.enrolid
          and c.admdate = b.svcdate ) ;
         


-----------------------------------
------------LVL 5
-------------------------------------

drop table if exists dev.truven_covid_lvl5;


select distinct enrolid 
into dev.truven_covid_lvl5
from dev.truven_covid_inp_dx ;

select count(distinct enrolid) from dev.truven_covid_lvl5;


/*

-- make sure count is same when joined with inpatient table
select count(distinct a.enrolid)
from dev.truven_covid_inp_dx a
join truven.ccaei b on a.enrolid = b.enrolid
 				and a.caseid = b.caseid;

*/ -----------------------------------
------------LVL 6
-------------------------------------

drop table if exists dev.truven_covid_lvl6;


select distinct a.enrolid into dev.truven_covid_lvl6
from dev.truven_covid_inp_dx a
join truven.ccaes b on a.enrolid = b.enrolid
join truven.ccaei c on a.enrolid = c.enrolid
and a.caseid = c.caseid
and b.caseid = c.caseid
where b.proc1 in ('94660',
                  '94662',
                  '94779')
   or b.revcode in ('0270',
                    '0175',
                    '0998',
                    '0272')
   and b.svcdate between '2020-01-01' and '2020-12-31' ;

-----------------------------------
---------- create normalized ICD and CPT procedure code list
  -------- pproc is CPT, rest are ICD
-----------------------------------

drop table if exists dev.normalized_proc_inp;

select distinct a.enrolid,
                a.caseid,
                unnest(array[a.pproc, a.proc1, a.proc2, a.proc3, a.proc4, a.proc1, a.proc2, 
                a.proc3, a.proc4, a.proc5, a.proc6, a.proc7, a.proc8, a.proc9, a.proc10, a.proc11, 
                a.proc12, a.proc12, a.proc13, a.proc14, a.proc15]) as proc 
                into dev.normalized_proc_inp
from truven.ccaei a
join dev.truven_covid_inp_dx b 
			on a.enrolid = b.enrolid
			and a.caseid = b.caseid ;

--select count(distinct enrolid) from dev.normalized_proc_inp;
 -----------------------------------
------------LVL 7
-------------------------------------

drop table if exists dev.truven_covid_lvl7;


select distinct enrolid 
into dev.truven_covid_lvl7
from
   (select enrolid
    from dev.normalized_proc_inp
    where proc in ('94002',
                   '94003',
                   '94004',
                   '94005',
                   '31500')
       or proc in ('5A1955Z',
                   '5A1935Z',
                   '5A1945Z')
       or proc like '5A093%'
       or proc like '5A094%'
       or proc like '5A095%'
union all 
    select a.enrolid
    from dev.truven_covid_inp_dx a
    join truven.ccaes b on a.enrolid = b.enrolid
    and a.caseid = b.caseid
    where b.revcode in ('0410') ) a ;

-----------------------------------
------------LVL 8
-------------------------------------

drop table if exists dev.truven_covid_lvl8;


select distinct enrolid 
into dev.truven_covid_lvl8
from
   (select enrolid
    from dev.normalized_proc_inp
    where proc between '33946' and '33959'
       or proc between '33962' and '33966'
       or proc between '33984' and '33989'
       or proc = '33969'
       or proc = '5A1D00Z'
       or proc = '5A1D60Z'
       or proc like '3E1M39Z'
       or proc = '5A1522F' 
          or proc = '5A1522G'  
          or proc = '5A1522H'  
union all 
    select a.enrolid
    from dev.truven_covid_inp_dx a
    join truven.ccaes b on a.enrolid = b.enrolid
    and a.caseid = b.caseid
    where b.revcode between '0800' and '0809' ) a ;

select count(*) from dev.truven_covid_lvl8; --1593
-----------------------------------
------------LVL 9
-------------------------------------   
   
drop table if exists dev.truven_covid_lvl9;

with caseids as 
(
select distinct enrolid,
                    caseid
 from dev.truven_covid_inp_dx
 )
select distinct a.enrolid
into   dev.truven_covid_lvl9
  from caseids a
  join truven.ccaei b 
    on a.enrolid = b.enrolid 
   and b.caseid = a.caseid
where  dstatus is null
;

   
drop table if exists dev.truven_covid_severity;




select distinct enrolid,
                Max(lvl) as severity 
                into dev.truven_covid_severity
from
   (select distinct enrolid,
                    1 as lvl
    from dev.truven_covid_lvl1
    union select distinct enrolid,
                          2 as lvl
    from dev.truven_covid_lvl2
    union select distinct enrolid,
                          3 as lvl
    from dev.truven_covid_lvl3
    union select distinct enrolid,
                          4 as lvl
    from dev.truven_covid_lvl4
    union select distinct enrolid,
                          5 as lvl
    from dev.truven_covid_lvl5
    union select distinct enrolid,
                          6 as lvl
    from dev.truven_covid_lvl6
    union select distinct enrolid,
                          7 as lvl
    from dev.truven_covid_lvl7
    union select distinct enrolid,
                          8 as lvl
    from dev.truven_covid_lvl8    
    union
        select distinct enrolid,
        									9 as lvl
        from   dev.truven_covid_lvl9) a
group by enrolid ;

select count(*) from dev.truven_covid_severity;
select count(distinct enrolid) from dev.truven_covid_severity;



select severity,
       count(*)
from dev.truven_covid_severity
group by severity ;


/*
drop table if exists dev.truven_covid_severity_netpay;

select * from dev.truven_covid_severity;


with payments as
   (select distinct a.enrolid,
                    a.caseid,
                    a.totnet
    from truven.ccaei a
    join dev.truven_covid_inp_dx b on a.enrolid = b.enrolid
    and a.caseid = b.caseid
    where a.totnet > 0)
select enrolid,
       sum(totnet) as total_cost 
into dev.truven_covid_severity_netpay
from payments
group by enrolid ;

select * from dev.truven_covid_severity_netpay;



drop table if exists dev.truven_covid_severity_netpay;


select severity,
       count(distinct a.enrolid) as people,
       median(total_cost),
       min(total_cost),
       max(total_cost) as max,
       sum(total_cost) as total_cost,
       sum(total_cost) / count(distinct a.enrolid) as avg_cost
from dev.truven_covid_severity_netpay a
join dev.truven_covid_severity b on a.enrolid = b.enrolid
group by severity
order by severity;


select distinct a.enrolid,
                a.caseid,
                a.totnet
from truven.ccaei a
join dev.truven_covid_inp_dx b on a.enrolid = b.enrolid
and a.caseid = b.caseid
order by totnet asc;
*/


--cleanup 
/*
drop table if exists dev.truven_ccaeo_test;
drop table if exists dev.truven_covid_inp2_dx;
drop table if exists dev.truven_covid_inp_dx;
drop table if exists dev.truven_covid_lvl1;
drop table if exists dev.truven_covid_lvl2;
drop table if exists dev.truven_covid_lvl3;
drop table if exists dev.truven_covid_lvl4;
drop table if exists dev.truven_covid_lvl5;
drop table if exists dev.truven_covid_lvl6;
drop table if exists dev.truven_covid_lvl7;
drop table if exists dev.truven_covid_lvl8;
drop table if exists dev.truven_covid_lvl9;
drop table if exists dev.truven_covid_out_dx;
drop table if exists dev.truven_covid_severity;
drop table if exists dev.truven_covid_severity_netpay;
*/