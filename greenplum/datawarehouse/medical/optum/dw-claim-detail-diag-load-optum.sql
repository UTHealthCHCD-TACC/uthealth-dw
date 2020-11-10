
analyze data_warehouse.claim_header;

analyze data_warehouse.claim_detail;

delete from data_warehouse.claim_diag where data_source in ('optz', 'optd');

select distinct data_source from data_warehouse.claim_diag cd ;

--Optum load: 
--explain
insert into data_warehouse.claim_diag(data_source, year, uth_claim_id, uth_member_id, claim_sequence_number, date, diag_cd, diag_position, icd_type, poa_src, data_year )
select distinct d.data_source, d.year, d.uth_claim_id, d.uth_member_id, d.claim_sequence_number, diag.fst_dt, diag.diag, diag.diag_position, diag.icd_flag, diag.poa, diag.year 
from data_warehouse.claim_detail d
join  optum_dod.diagnostic diag 
	on diag.clmid =d.claim_id_src::text 
	and diag.patid::text=d.member_id_src 
	and diag.fst_dt=d.from_date_of_service
where d.data_source='optd'
and diag.year = 2009;


delete from data_warehouse.claim_diag where diag_cd is null;

vacuum analyze data_warehouse.claim_diag;


--Verify
select data_source, count(*)
from data_warehouse.claim_diag d
group by 1
order by 1;

select data_source, year, count(*)
from data_warehouse.claim_diag d
group by 1, 2
order by 1, 2;

--Missing diags???
select count(distinct h.uth_claim_id) 
--select h.uth_claim_id, h.claim_id_src, h.member_id_src
from dw_qa.claim_header h
left outer join dw_qa.claim_detail_diag diag on h.uth_claim_id=diag.uth_claim_id
where h.data_source = 'optd' and diag.diag_cd is null;


@set clmid = '2449466899'
@set patid = 33171032334

select *
from optum_dod.diagnostic
where patid=:patid
order by clmid desc;


--delete from dw_qa.claim_detail_diag where uth_claim_id in (select uth_claim_id from dw_qa.claim_header where data_source='optz');

select distinct icd_flag
from optum_zip.diagnostic;

select distinct datatyp
from truven.mdcrs
limit 1;

select distinct poadx1
from truven.ccaei;

CREATE INDEX ix_claim_detail_diag_diag_cd_bitmap
ON dw_qa.claim_detail_diag
USING bitmap (diag_cd);

vacuum analyze dw_qa.claim_detail_diag;


delete from data_warehouse.claim_diag where data_source is null;

