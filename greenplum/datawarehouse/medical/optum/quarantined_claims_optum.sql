truncate quarantine.uth_claim_ids;

--Optimized table for running further analysis
drop table dev.qtemp_all;
create table dev.qtemp_all 
WITH (
	appendonly=true, orientation=column
) as
select year, clmid, patid, clmseq, coalesce(conf_id, '0') as conf_id
from optum_dod.medical
distributed by (year, clmid, patid, clmseq);

analyze dev.qtemp_all;

/*
 * Diff conf_id
 */

drop table quarantine.optum_multiple_confs;
create table quarantine.optum_multiple_confs 
WITH (
	appendonly=true, orientation=column
) as
select year, clmid, patid, clmseq
from dev.qtemp_all 
group by 1, 2, 3, 4
having count(distinct conf_id) > 1
distributed randomly;


analyze quarantine.optum_multiple_confs;
analyze dw_qa.dim_uth_member_id;
analyze dw_qa.dim_uth_claim_id;

--Check for missing uth_claim_ids
select m.clmid, m.patid
from quarantine.optum_multiple_confs m
left outer join dw_qa.dim_uth_claim_id uth on m.clmid=uth.claim_id_src and m.patid::text=uth.member_id_src and 'optz'=uth.data_source
where uth.uth_claim_id is null;

--Load
insert into quarantine.uth_claim_ids(data_source, uth_claim_id, note)
select distinct 'optz', uth.uth_claim_id, 'multiple confinement records'
from quarantine.optum_multiple_confs m
join dw_qa.dim_uth_claim_id uth on m.clmid=uth.claim_id_src and m.patid::text=uth.member_id_src and 'optz'=uth.data_source
left join quarantine.uth_claim_ids qid on uth.uth_claim_id = qid.uth_claim_id 
where qid.uth_claim_id is null; --Don't add already added records



--Dupe records, diff conf_id/pat_planid
@set clmid = '3787091250'
@set patid = 33069939913


@set clmid = '4160819810'
@set patid = 33038028011

/*
 * Diff clmseq==001
 */ 

drop table quarantine.optum_dupe_clmseq;
create table quarantine.optum_dupe_clmseq 
WITH (
	appendonly=true, orientation=column
) as
select year, clmid, patid, clmseq, count(*) as cnt
from dev.qtemp_all
group by 1, 2, 3, 4
having count(*) > 1 and count(distinct conf_id)=1;

analyze quarantine.optum_dupe_clmseq;

select count(*)
from quarantine.optum_dupe_clmseq
limit 10;

--Check for missing uth_claim_ids
select m.clmid, m.patid
from quarantine.optum_dupe_clmseq m
left outer join dw_qa.dim_uth_claim_id uth on m.clmid=uth.claim_id_src and m.patid::text=uth.member_id_src and 'optz'=uth.data_source
where uth.uth_claim_id is null;

--Load
insert into quarantine.uth_claim_ids(data_source, uth_claim_id, note)
select distinct 'optz', uth.uth_claim_id, 'dupe clmseq'
from quarantine.optum_dupe_clmseq m
join dw_qa.dim_uth_claim_id uth on m.clmid=uth.claim_id_src and m.patid::text=uth.member_id_src and 'optz'=uth.data_source
left join quarantine.uth_claim_ids qid on uth.uth_claim_id = qid.uth_claim_id 
where qid.uth_claim_id is null; --Don't add already added records;


--Scratch

analyze quarantine.uth_claim_ids;

select data_source, note, count(distinct uth_claim_id), count(*)
from quarantine.uth_claim_ids 
group by 1, 2;

--Specific examples
@set clmid = '1029863351'
@set patid = 33013882207

execute optum_use_case(:clmid, :patid);

@set clmid = '1729040027'
@set patid = 33003521903

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
from optum_dod.medical m
left join optum_dod.ref_admit_type rat on m.admit_type::varchar=rat.key::varchar
left join optum_dod.ref_admit_channel rac on m.admit_chan::varchar=rac.key::varchar and case when m.admit_chan='4' then rac.type_id=4 else rac.type_id is null end
left join optum_dod.diagnostic d on m.clmid=d.clmid and m.fst_dt=d.fst_dt and d.diag_position=1
left join optum_dod.confinement con on m.conf_id=con.conf_id
left join optum_dod.procedure p on m.clmid=p.clmid and m.fst_dt = p.fst_dt
left join optum_dod.facility_detail fd on m.clmid=fd.clmid
left join reference_tables.hcpcs h on m.proc_cd=h.code
left join reference_tables.cms_proc_codes c on m.proc_cd=c.code
left join reference_tables.icd_10 i on d.diag=i.icd_10
where m.clmid=$1 and m.patid=$2
order by m.clmseq;