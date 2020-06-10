

/*
 * Remove old records
 */
delete from dw_qa.admission_header where data_source like 'opt%';

/*
 * Stage temp data for optimized joins of raw tables
 */
create table dev.optd_conf (like optum_dod.confinement)
distributed by (patid);

insert into dev.optd_conf
select * from optum_dod.confinement;

analyze dev.optd_conf;

create table dev.optd_med (like optum_dod.medical)
distributed by (patid);

insert into dev.optd_med
select * from optum_dod.medical;

analyze dev.optd_med;

/*
 * We assume the matching records exist in dim_uth_claim_id
 */
--Optum load: 
-- Full years = 1000 seconds = 17m
insert into dw_qa.admission_header(data_source, year, uth_member_id, member_id_src, uth_admission_id, admission_id_src,
admit_date, discharge_date,
admit_type, admit_channel, discharge_status,
primary_diagnosis_cd, primary_icd_proc_cd,
total_charge_amount, total_allowed_amount, total_paid_amount)
select 'optz', c.year, uthc.uth_member_id, c.patid, uthc.uth_admission_id, c.conf_id,
c.admit_date, c.disch_date,
rat.admit_type, rac.admit_source, c.dstatus,
c.diag1, c.proc1,
c.charge, c.std_cost, null
from dev.optz_conf c
join dev.optz_med m on c.patid=m.patid and c.conf_id=m.conf_id
join data_warehouse.dim_uth_admission_id uthc on uthc.data_source='optz' and c.conf_id=uthc.admission_id_src and c.patid::text=uthc.member_id_src 
left join reference_tables.ref_admit_type rat on m.admit_type::text=rat.admit_type_cd::text
left join reference_tables.ref_admit_source rac on m.admit_chan::text=rac.admit_source_cd::text;
--left outer join quarantine.uth_claim_ids q on uthc.uth_claim_id=q.uth_claim_id
--where q.uth_claim_id is null

/*
 * SCRATCH
 */
select count(*), count(distinct conf_id)
from dev2016.optum_dod_confinement;

select count(*), count(distinct admission_id_src)
from data_warehouse.dim_uth_admission_id;

select *
from dev2016.optum_dod_confinement
limit 10;

select distinct dstatus from dev2016.optum_dod_confinement odc2 ;

select conf_id, count(*)
from dev2016.optum_dod_confinement odc 
where conf_id is not null
group by 1
having count(*)=1;

select *
from reference_tables.ref_admit_type;

select *
from reference_tables.ref_admit_source;