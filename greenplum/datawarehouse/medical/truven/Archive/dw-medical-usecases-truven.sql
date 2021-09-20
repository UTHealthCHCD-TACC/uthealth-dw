/*
* Examine various claim 'stories' at the raw data level.
*/

SELECT * 
FROM dw_qa.dim_uth_claim_id
WHERE uth_member_id = 339826076 and uth_claim_id = 9368354758;

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
'SERVICES:',
p.value as place_of_service,
o.*
from truven_ccaeo o
left join truven.ref_place_of_service p on o.stdplac=p.key
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

/*
 * Case 3: Random Proc Case
 */
@set clmid = 910974166.0
@set enrolid = 268861401.0
--Inpatient
select distinct c.code as proc_code, c.desc, s.dx1, ic.description dx1_desc, 
'ADMISSIONS:', 
atyp.value as admit_type,
ds.value as discharge_status,
i.*,
'SERVICES:', 
p.value as place_of_service,
st.value as service_type,
s.*
from truven.mdcri s 
left join truven.ref_place_of_service p on s.stdplac=p.key
left join truven.mdcri i on s.caseid=i.caseid and s.enrolid=i.enrolid
left join truven.ref_admit_type atyp on i.admtyp=atyp.key
left join truven.ref_discharge_status ds on i.dstatus=ds."key"
left join truven.ref_service_type st on s.svcscat=st.key
left join reference_tables.cms_proc_codes c on s.proc1=c.code and coalesce(s.procmod, 'none')=coalesce(c.mod, 'none')
left join reference_tables.icd_10 ic on s.dx1=ic.icd_10
where s.msclmid=:clmid 
and s.enrolid=:enrolid
order by s.seqnum;

-- Individual tables


/*
 * ICD Procs?
 */



/*
 * End Procedure
 */


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