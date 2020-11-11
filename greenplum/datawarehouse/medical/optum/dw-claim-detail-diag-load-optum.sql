
analyze data_warehouse.claim_header;

analyze data_warehouse.claim_detail;

delete from data_warehouse.claim_diag where data_source in ('optz', 'optd');

select distinct data_source from data_warehouse.claim_diag cd ;

--Optum load: 
--explain

--optz
insert into data_warehouse.claim_diag(data_source, year, uth_claim_id, uth_member_id, claim_sequence_number, date, diag_cd, diag_position, icd_type, poa_src, data_year )
select distinct d.data_source, d.year, d.uth_claim_id, d.uth_member_id, d.claim_sequence_number, diag.fst_dt, diag.diag, diag.diag_position, diag.icd_flag, diag.poa, diag.year 
from data_warehouse.claim_detail d
join  optum_zip.diagnostic diag 
	on diag.clmid =d.claim_id_src::text 
	and diag.patid::text=d.member_id_src 
	and diag.fst_dt=d.from_date_of_service
where d.data_source='optz'
and diag.year = 2020;


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


