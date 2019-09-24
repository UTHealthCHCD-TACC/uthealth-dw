/*
* Examine various claim 'stories' at the raw data level.
*/

/*
 * Case 1: Abnormal weight loss, ct scans + drugs
 */
@set clmid = 118318
@set enrolid = 1589466401

/*
* Case 2: Bipolor venipuncture/blood test
*/
@set clmid = 1466020
@set enrolid = 602902

/*
 * Queries
 */

--Outpatient
select distinct c.code as proc_code, c.desc, o.dx1, i.description dx1_desc, 
o.*
from truven_ccaeo o
left join reference_tables.cms_proc_codes c on o.proc1=c.code and coalesce(o.procmod, 'none')=coalesce(c.mod, 'none')
left join reference_tables.icd_10 i on o.dx1=i.icd_10
where o.msclmid=:clmid and enrolid=:enrolid
order by o.seqnum;

/*
 * Case 1: Bypass graft
 */
@set clmid = 1187600
@set enrolid = 14744002

/*
 * Case 2: Malignant neoplasm prostate
 */
@set clmid = 266334
@set enrolid = 14516012

--Inpatient
select distinct c.code as proc_code, c.desc, o.dx1, i.description dx1_desc, 
'ADMISSIONS:', a.*,
'SERVICES:', o.*
from truven.ccaei a
join truven_ccaes o on a.caseid=o.caseid and a.enrolid=o.enrolid
left join reference_tables.cms_proc_codes c on o.proc1=c.code and coalesce(o.procmod, 'none')=coalesce(c.mod, 'none')
left join reference_tables.icd_10 i on o.dx1=i.icd_10
where o.msclmid=:clmid 
and o.enrolid=:enrolid
order by o.seqnum;

-- Individual tables
select *
from truven.ccaeo
where msclmid=:clmid
and enrolid=:enrolid;

select msclmid, enrolid
from truven_ccaeo

select msclmid, enrolid
from truven_ccaes


select *
from truven_ccaeo
where seqnum >= 6742528 and seqnum <= 6742530

create table truven_ccaeo_fix
as select distinct *
from truven_ccaeo;
drop table truven_ccaeo;
alter table truven_ccaeo_fix rename to truven_ccaeo;