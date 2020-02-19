
analyze dw_qa.claim_header;
analyze dw_qa.claim_detail;

--drop table if exists dw_qa.claim_detail_diag;

create table dw_qa.claim_detail_diag (
	uth_claim_id int8,
	claim_sequence_number int2,
	diagnosis_code text,
	diagnosis_sequence int2
) 
WITH (appendonly=true, orientation=column)
distributed by (uth_claim_id);



--Optum load: 
insert into dw_qa.claim_detail_diag(uth_claim_id, claim_sequence_number, diagnosis_code, diagnosis_sequence)
select distinct uth.uth_claim_id, d.claim_sequence_number, diag.diag, diag.diag_position
from dw_qa.claim_detail d
join dw_qa.claim_header h on d.uth_claim_id=h.uth_claim_id
join data_warehouse.dim_uth_claim_id uth on h.uth_claim_id=uth.uth_claim_id
join optum_zip.diagnostic diag on diag.clmid=h.claim_id_src and diag.patid::text=h.member_id_src and diag.year=uth.data_year and diag.fst_dt=d.from_date_of_service
where h.data_source='optz';

limit 10;

-- Diagnostics

analyze dw_qa.claim_detail_diag;

--Verify
select data_source, count(*), count(distinct d.uth_claim_id)
from dw_qa.claim_detail_diag d
join dw_qa.claim_detail l on d.claim_sequence_number=l.claim_sequence_number and d.uth_claim_id=l.uth_claim_id
group by 1;

select data_source, count(*)
from dev.claim_header_optum
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

select distinct poa
from dev2016.optum_dod_diagnostic;

select distinct poadx1
from dev2016.truven_ccaei;

CREATE INDEX ix_claim_detail_diag_diag_cd_bitmap
ON dw_qa.claim_detail_diag
USING bitmap (diag_cd);

vacuum analyze dw_qa.claim_detail_diag;


