
analyze dw_qa.claim_header;
analyze dw_qa.claim_detail;

--drop table if exists dw_qa.claim_detail_diag;

create table dw_qa.claim_detail_diag (
	uth_claim_id int8,
	claim_sequence_number int2,
	date date,
	diag_cd text,
	diag_position int2,
	icd_type text,
	poa_src text
) 
WITH (appendonly=true, orientation=column)
distributed by (uth_claim_id);

--Remove Old
delete from dw_qa.claim_detail_diag where uth_claim_id in (select uth_claim_id from dw_qa.claim_header where data_source like 'opt%');

--Optum load: 
insert into dw_qa.claim_detail_diag(uth_claim_id, claim_sequence_number, date, diag_cd, diag_position, icd_type, poa_src)
select distinct uth.uth_claim_id, d.claim_sequence_number, diag.fst_dt, diag.diag, diag.diag_position, diag.icd_flag, diag.poa

explain
select count(*)
from dw_qa.claim_detail d
join dw_qa.claim_header h on d.uth_claim_id=h.uth_claim_id
join data_warehouse.dim_uth_claim_id uth on h.uth_claim_id=uth.uth_claim_id
join optum_zip_refresh.diagnostic diag on diag.clmid=h.claim_id_src and diag.patid::text=h.member_id_src and diag.year=uth.data_year and diag.fst_dt=d.from_date_of_service
where h.data_source='optz';

vacuum full dw_qa.claim_header;
analyze dw_qa.claim_header;

vacuum full dw_qa.claim_detail;
analyze dw_qa.claim_detail;

vacuum full dw_qa.claim_detail_diag;
analyze dw_qa.claim_detail_diag;

limit 10;

-- Diagnostics

analyze dw_qa.claim_detail_diag;

--Verify
select data_source, count(*), count(distinct d.uth_claim_id)
from dw_qa.claim_detail_diag_old d
join dw_qa.claim_detail l on d.claim_sequence_number=l.claim_sequence_number and d.uth_claim_id=l.uth_claim_id
group by 1;

select data_source, count(*)
from dw_qa.claim_header
group by 1;

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


