/*
* Examine various claim 'stories' at the raw data level.
*/

/*
 * Case 1: A dementia patient with mobility therapy sessions.
 */
@set clmid = '4111219171'

/*
* Case 2: Dorsalgia (upper back pain) radiology imaging
*/
@set clmid = '4084387213'

/*
 * Queries
 */
select distinct m.clmid, m.clmseq, d.clmid as has_diag, p.clmid as has_proc, fd.clmid as has_fd, 
m.proc_cd, h.code as hcpcs_code, h.short_desc as hcpcs_short_desc, c.code as cms_proc_code, c.desc as cms_proc_desc,
m.*
from optum_zip_medical m
left join optum_zip_diagnostic d on m.clmid=d.clmid and m.fst_dt=d.fst_dt
left join optum_zip_procedure p on m.clmid=p.clmid and m.fst_dt = p.fst_dt
left join optum_zip_facility_detail fd on m.clmid=fd.clmid
left join reference_tables.hcpcs h on m.proc_cd=h.code
left join reference_tables.cms_proc_codes c on m.proc_cd=c.code
where m.clmid=:clmid
order by m.clmseq;

-- Individual tables
select * from optum_zip_medical
where clmid=:clmid
order by clmseq, fst_dt;

select * from optum_zip_diagnostic
where clmid=:clmid
order by fst_dt, diag_position;

select * from optum_zip_procedure
where clmid=:clmid;

select * from optum_zip_facility_detail
where clmid=:clmid;

select * from reference_tables.hcpcs where code like '%97116%';