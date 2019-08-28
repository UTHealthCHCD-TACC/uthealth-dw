/*
* Examine various claim 'stories' at the raw data level.
*/

/*
 * Case 1: A dementia patient with mobility therapy sessions.
 */
@set clmid = 118318
@set enrolid = 1589466401

/*
* Case 2: Dorsalgia (upper back pain) radiology imaging
*/
@set clmid = 118318
@set enrolid = 1589466401

/*
 * Queries
 */
select distinct c.code as proc_code, c.desc, o.dx1, i.description dx1_desc, 
o.*
from truven_ccaeo o
left join reference_tables.cms_proc_codes c on o.proc1=c.code and coalesce(o.procmod, 'none')=coalesce(c.mod, 'none')
left join reference_tables.icd_10 i on o.dx1=i.icd_10
where o.msclmid=:clmid and enrolid=:enrolid
order by o.seqnum;

-- Individual tables
select *
from truven.ccaeo
where msclmid=:clmid
and enrolid=:enrolid;