---Optum diag - rewritten 4/17/2021 Will Coughlin

--create copy of diagnosis table and distribute on patid as text field
drop table dev.wc_optz_diag;

create table dev.wc_optz_diag
with(appendonly=true,orientation=column)
as 
	select patid::text as member_id_src, *
	from optum_zip.diagnostic d 
distributed by (member_id_src);


vacuum analyze dev.wc_optz_diag;


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
select  b.data_source, extract(year from a.fst_dt) as cal_yr, b.uth_member_id, b.uth_claim_id, 1 as clm_seq, a.fst_dt, 
        a.diag, a.diag_position, a.icd_flag, a.poa, extract(year from a.fst_dt) as fsc_yr
from dev.wc_optz_diag a 
   join dev.wc_optz_uth_claim b 
      on b.member_id_src = a.member_id_src
     and b.claim_id_src = a.clmid 
     and b.data_source = 'optz'
 ;    

     
     

select * from data_warehouse.claim_header ch where uth_claim_id = 7392830758;


select * from optum_zip.medical m where m.patid::text = '560499820219554' and m.clmid = 'O8VV8NN8FO'

select * from optum_zip.diagnostic m where m.patid::text = '560499820219554' and m.clmid = 'O8VV8NN8FO'


--optd
insert into data_warehouse.claim_diag(data_source, year, uth_claim_id, uth_member_id, claim_sequence_number, date, diag_cd, diag_position, icd_type, poa_src, data_year )
select distinct d.data_source, d.year, d.uth_claim_id, d.uth_member_id, d.claim_sequence_number, diag.fst_dt, diag.diag, diag.diag_position, diag.icd_flag, diag.poa, diag.year 
from data_warehouse.claim_detail d
join  optum_dod.diagnostic diag 
	on diag.clmid =d.claim_id_src::text 
	and diag.patid::text=d.member_id_src 
	and diag.fst_dt=d.from_date_of_service
where d.data_source='optd'
and diag.year = 2009;


select distinct data_source from data_warehouse.claim_diag;


delete from data_warehouse.claim_diag where diag_cd is null;

vacuum analyze data_warehouse.claim_diag;


--Verify
select data_source, year, count(*)
from data_warehouse.claim_diag d
group by 1, 2
order by 1, 2;


