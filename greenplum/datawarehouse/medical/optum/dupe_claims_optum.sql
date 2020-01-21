/*
 * Diff conf_id
 */
drop table quarantine.optum_dupe_claims;
create table quarantine.optum_dupe_claims 
WITH (
	appendonly=true, orientation=column
) as
select m.clmid, m.patid, m.conf_id
from optum_dod.medical m
where cast(m.clmseq as int)=1
distributed randomly;

--Total duplicates: 510
explain
select clmid, patid, count(*) as cnt, count(distinct conf_id) as uniq_conf_id
from quarantine.optum_dupe_claims
group by 1, 2
having count(distinct conf_id) > 1;



--Dupe records, diff conf_id/pat_planid
@set clmid = '4237722577'
@set patid = '33061788874'


@set clmid = '4160819810'
@set patid = '33038028011'

/*
 * Diff clmseq==001
 */ 
explain
select clmid, patid, count(*) as cnt
from optum_dupe_claims
group by 1, 2
having count(*) > 1 and count(distinct conf_id)=1;

--Specific examples
@set clmid = '4395192264'
@set patid = 33063749079

execute optum_use_case(:clmid, :patid);

/*
 * Queries
 */
DEALLOCATE optum_use_case;
prepare optum_use_case(varchar, int8) as
select distinct m.clmid, m.clmseq, m.fst_dt,
d.clmid as has_diag, p.clmid as has_proc, fd.clmid as has_fd, 
m.prov_par in_network,
m.proc_cd, h.code as hcpcs_code, h.short_desc as hcpcs_short_desc, c.code as cms_proc_code, c.desc as cms_proc_desc,
d.diag, i.icd_10, i.description as primary_diag,
rat.value as admit_type_val,
rac.value_derived as admit_channel_val,
'MEDICAL:',
m.*,
'CONFINEMENT:',
con.*,
'DIAGNOSTIC:',
d.*
from dev2016.optum_dod_medical m
left join optum_dod.ref_admit_type rat on m.admit_type::varchar=rat.key::varchar
left join optum_dod.ref_admit_channel rac on m.admit_chan::varchar=rac.key::varchar and case when m.admit_chan='4' then rac.type_id=4 else rac.type_id is null end
left join optum_dod_diagnostic d on m.clmid=d.clmid and m.fst_dt=d.fst_dt and d.diag_position=1
left join optum_dod_confinement con on m.conf_id=con.conf_id
left join optum_dod_procedure p on m.clmid=p.clmid and m.fst_dt = p.fst_dt
left join optum_dod_facility_detail fd on m.clmid=fd.clmid
left join reference_tables.hcpcs h on m.proc_cd=h.code
left join reference_tables.cms_proc_codes c on m.proc_cd=c.code
left join reference_tables.icd_10 i on d.diag=i.icd_10
where m.clmid=$1 and m.patid=$2
order by m.clmseq;