

/*
 * Remove old records
 */
delete from dw_qa.admission_header where data_source like 'opt%';

/*
 * Stage temp data for optimized joins of raw tables
 */

--
drop table dev.optz_conf;
create table dev.optz_conf (like optum_zip.confinement)
WITH (appendonly=true, orientation=column)
distributed by (patid);

insert into dev.optz_conf
select * from optum_zip.confinement;

analyze dev.optz_conf;

--
drop table dev.optz_med;
create table dev.optz_med (like optum_zip.medical)
WITH (appendonly=true, orientation=column)
distributed by (patid);

insert into dev.optz_med
select * from optum_zip.medical;

analyze dev.optz_med;

select count(*) from dev.optz_med;

--truncate dw_qa.admission_header;

/*
 * We assume the matching records exist in dim_uth_claim_id
 */
--Optum load: 
-- Full years = 290 seconds = 5m
insert into dw_qa.admission_header(data_source, year, uth_member_id, member_id_src, uth_admission_id, admission_id_src,
admit_date, discharge_date, discharge_status,
primary_diagnosis_cd, primary_icd_proc_cd,
total_charge_amount, total_allowed_amount, --total_paid_amount,
admit_type, admit_channel)
select 'optz', c.year, uthc.uth_member_id, c.patid, uthc.uth_admission_id, c.conf_id,
c.admit_date, c.disch_date, c.dstatus,
c.diag1, c.proc1,
c.charge, c.std_cost, --null,
max(rat.admit_type), max(rac.admit_source)
from dev.optd_conf c
join dev.optd_med m on c.patid=m.patid and c.conf_id=m.conf_id
join data_warehouse.dim_uth_admission_id uthc on uthc.data_source='optz' and c.conf_id=uthc.admission_id_src and c.patid::text=uthc.member_id_src 
left join reference_tables.ref_admit_type rat on m.admit_type::text=rat.admit_type_cd::text
left join reference_tables.ref_admit_source rac on m.admit_chan::text=rac.admit_source_cd::text
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13; --Deal with multiple medical records
--left outer join quarantine.uth_claim_ids q on uthc.uth_claim_id=q.uth_claim_id
--where q.uth_claim_id is null

/*
 * SCRATCH
 */
select data_source, year, count(*)
from dw_qa.admission_header ah 
group by 1, 2
order by 1, 2;

select count(*), count(distinct conf_id)
from dev2016.optum_zip_confinement;


create table dev.optum_conf_dupe
as
select *
from dev.optd_med
where patid=33024829716 and conf_id='L6Z4K4TK4OK4L';

select *
from dev.optum_conf_dupe;

select distinct admit_date
from dev.optum_conf_dupe;

select min(admit_type)
from dev.optum_conf_dupe;


select count(*), count(distinct admission_id_src)
from data_warehouse.dim_uth_admission_id;

select *
from dev2016.optum_zip_confinement
limit 10;

select distinct dstatus from dev2016.optum_zip_confinement odc2 ;

select conf_id, count(*)
from dev2016.optum_zip_confinement odc 
where conf_id is not null
group by 1
having count(*)=1;

select *
from reference_tables.ref_admit_type;

select *
from reference_tables.ref_admit_source;

select * from dw_qa.admission_header where uth_admission_id = 132689515;

select * from optum_zip.confinement where conf_id = 'L6Z4K4TK4OK4L';

select * from data_warehouse.dim_uth_admission_id where uth_admission_id = 132689515; 


