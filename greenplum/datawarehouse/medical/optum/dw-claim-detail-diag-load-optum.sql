---Optum diag - rewritten 4/17/2021 Will Coughlin

---******************************************************************************************************************
------ Optum Zip - optz
---******************************************************************************************************************

--create copy of diagnosis table and distribute on patid as text field
drop table dev.wc_optz_diag;

create table dev.wc_optz_diag
with(appendonly=true,orientation=column)
as 
	select patid::text as member_id_src, *
	from optum_zip.diagnostic d 
distributed by (member_id_src);


select count(*), year 
from optum_zip.diagnostic
group by year order by year;

vacuum analyze dev.wc_optz_diag;

select count(*), year  from dev.wc_optz_diag group by year ;

select * from data_warehouse.dim_uth_member_id where member_id_src = '560499808606893'

select * from data_warehouse.member_enrollment_yearly mey where uth_member_id = 190388598

select * from data_warehouse.claim_diag cd where uth_member_id = 190388598;


---create copy uth claims with optz only and distribute on member id src
drop table if exists dev.wc_optz_uth_claim;

create table dev.wc_optz_uth_claim
with(appendonly=true,orientation=column)
as select * from data_warehouse.dim_uth_claim_id where data_source = 'optz'
distributed by (member_id_src);


vacuum analyze dev.wc_optz_uth_claim;


---work table to load
drop table dev.wc_claim_diag_optz;

create table dev.wc_claim_diag_optz
with(appendonly=true,orientation=column)
as select * from data_warehouse.claim_diag limit 0
distributed by (uth_member_id);

--optz
insert into dev.wc_claim_diag_optz
(data_source, year, uth_claim_id, uth_member_id, claim_sequence_number, from_date_of_service, diag_cd, diag_position, icd_type, poa_src, fiscal_year )
select  b.data_source, extract(year from a.fst_dt) as cal_yr,  b.uth_claim_id,b.uth_member_id, 1 as clm_seq, a.fst_dt, 
        a.diag, a.diag_position, a.icd_flag, a.poa, extract(year from a.fst_dt) as fsc_yr
from dev.wc_optz_diag a 
   join dev.wc_optz_uth_claim b 
      on b.member_id_src = a.member_id_src
     and b.claim_id_src = a.clmid 
     and b.data_source = 'optz'
 ;    

---validate
vacuum analyze dev.wc_claim_diag_optz;

select * from dev.wc_claim_diag_optz where year = 2018;

select * from data_warehouse.member_enrollment_yearly mey where uth_member_id = 206307061;

select * from data_warehouse.dim_uth_member_id dumi where uth_member_id = 7601799515

delete from data_warehouse.claim_diag where data_source = 'optz';

insert into data_warehouse.claim_diag 
select * from dev.wc_claim_diag_optz;
;

select * from data_warehouse.claim_diag cd where data_source = 'optz'
     
---******************************************************************************************************************
------ Optum DoD - optd
---******************************************************************************************************************     

--create copy of diagnosis table and distribute on patid as text field
drop table dev.wc_optd_diag;

create table dev.wc_optd_diag
with(appendonly=true,orientation=column)
as 
	select patid::text as member_id_src, *
	from optum_dod.diagnostic d 
distributed by (member_id_src);


vacuum analyze dev.wc_optd_diag;


---create copy uth claims with optz only and distribute on member id src
drop table if exists dev.wc_optd_uth_claim;

create table dev.wc_optd_uth_claim
with(appendonly=true,orientation=column)
as select * from data_warehouse.dim_uth_claim_id where data_source = 'optd'
distributed by (member_id_src);


vacuum analyze dev.wc_optd_uth_claim;


---work table to load
drop table dev.wc_claim_diag_optd;

create table dev.wc_claim_diag_optd
with(appendonly=true,orientation=column)
as select * from data_warehouse.claim_diag limit 0
distributed by (uth_member_id);

select * from dev.wc_claim_diag_optd

--optd
insert into dev.wc_claim_diag_optd
(data_source, year, uth_member_id, uth_claim_id, claim_sequence_number, from_date_of_service, diag_cd, diag_position, icd_type, poa_src, fiscal_year )
select  b.data_source, extract(year from a.fst_dt) as cal_yr, b.uth_member_id, b.uth_claim_id, 1 as clm_seq, a.fst_dt, 
        a.diag, a.diag_position, a.icd_flag, a.poa, extract(year from a.fst_dt) as fsc_yr
from dev.wc_optd_diag a 
   join dev.wc_optd_uth_claim b 
      on b.member_id_src = a.member_id_src
     and b.claim_id_src = a.clmid 
 ;    

vacuum analyze dev.wc_claim_diag_optd;

select * from dev.wc_claim_diag_optd;

select * from data_warehouse.member_enrollment_yearly mey where uth_member_id = 115860125;


---delete old records
delete from data_warehouse.claim_diag where data_source = 'optd';


--load optd
insert into data_warehouse.claim_diag 
select * from dev.wc_claim_diag_optd;

---vacc analyze
vacuum analyze data_warehouse.claim_diag;

--validate
select count(*), data_source, year 
from data_warehouse.claim_diag
group by data_source ,"year" 
order by data_source , year ;



select * from data_warehouse.claim_diag cd where diag_cd is null;


---cleanup

drop table dev.wc_claim_diag_optd ;

drop table dev.wc_optd_diag ;

drop table dev.wc_optd_uth_claim ;


drop table dev.wc_claim_diag_optz ;

drop table dev.wc_optz_diag ;

drop table dev.wc_optz_uth_claim ;
