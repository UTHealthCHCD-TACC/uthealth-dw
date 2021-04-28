---Optum icd proc - rewritten 4/18/2021 Will Coughlin


---******************************************************************************************************************
------ Optum Zip - optz
---******************************************************************************************************************


--create copy of icd proc table and distribute on patid as text field
drop table dev.wc_optz_proc;

create table dev.wc_optz_proc
with(appendonly=true,orientation=column)
as 
	select patid::text as member_id_src, *
	from optum_zip."procedure" 
distributed by (member_id_src);


vacuum analyze dev.wc_optz_proc;


---create copy uth claims with optz only and distribute on member id src
drop table if exists dev.wc_optz_uth_claim;

create table dev.wc_optz_uth_claim
with(appendonly=true,orientation=column)
as select * from data_warehouse.dim_uth_claim_id where data_source = 'optz'
distributed by (member_id_src);


vacuum analyze dev.wc_optz_uth_claim;

---work table to load
drop table dev.wc_claim_proc_optz;

create table dev.wc_claim_proc_optz
with(appendonly=true,orientation=column)
as select * from data_warehouse.claim_icd_proc limit 0
distributed by (uth_member_id);


--load optz working table
insert into dev.wc_claim_proc_optz  (data_source, year, uth_claim_id, uth_member_id, claim_sequence_number, from_date_of_service, 
      								 proc_cd, proc_position, icd_type, fiscal_year )
select  b.data_source, extract(year from a.fst_dt) as cal_yr, b.uth_member_id, b.uth_claim_id, 1 as clm_seq, a.fst_dt, 
        a.proc, a.proc_position, a.icd_flag, extract(year from a.fst_dt) as fsc_yr
from dev.wc_optz_proc a 
   join dev.wc_optz_uth_claim b 
      on b.member_id_src = a.member_id_src
     and b.claim_id_src = a.clmid 
 ;    



---******************************************************************************************************************
------ Optum DoD - optd
---******************************************************************************************************************


--create copy of icd proc table and distribute on patid as text field
drop table dev.wc_optd_proc;

create table dev.wc_optd_proc
with(appendonly=true,orientation=column)
as 
	select patid::text as member_id_src, *
	from optum_dod."procedure" 
distributed by (member_id_src);


vacuum analyze dev.wc_optd_proc;


---create copy uth claims with optd only and distribute on member id src
drop table if exists dev.wc_optd_uth_claim;

create table dev.wc_optd_uth_claim
with(appendonly=true,orientation=column)
as select * from data_warehouse.dim_uth_claim_id where data_source = 'optd'
distributed by (member_id_src);


vacuum analyze dev.wc_optd_uth_claim;

---work table to load
drop table dev.wc_claim_proc_optd;

create table dev.wc_claim_proc_optd
with(appendonly=true,orientation=column)
as select * from data_warehouse.claim_icd_proc limit 0
distributed by (uth_member_id);


--load optd working table
insert into dev.wc_claim_proc_optd  (data_source, year, uth_claim_id, uth_member_id, claim_sequence_number, from_date_of_service, 
      								 proc_cd, proc_position, icd_type, fiscal_year )
select  b.data_source, extract(year from a.fst_dt) as cal_yr, b.uth_member_id, b.uth_claim_id, 1 as clm_seq, a.fst_dt, 
        a.proc, a.proc_position, a.icd_flag, extract(year from a.fst_dt) as fsc_yr
from dev.wc_optd_proc a 
   join dev.wc_optd_uth_claim b 
      on b.member_id_src = a.member_id_src
     and b.claim_id_src = a.clmid 
 ;    
 
---******************************************************************************************************************
------ Validate
---******************************************************************************************************************

select count(*), year from optum_zip."procedure" group by year order by year;

select count(*), year from optum_dod."procedure" group by year order by year;

select count(*), year from dev.wc_claim_proc_optz group by year order by year;

select count(*), year from dev.wc_claim_proc_optd group by year order by year;

---******************************************************************************************************************
------ Production Load
---******************************************************************************************************************

--delete old recs
delete from data_warehouse.claim_icd_proc where data_source in ('optz','optd');

insert into data_warehouse.claim_icd_proc 
select * from dev.wc_claim_proc_optz;

insert into data_warehouse.claim_icd_proc 
select * from dev.wc_claim_proc_optd;


vacuum analyze data_warehouse.claim_icd_proc;







