--Claims with multiple uth_claim_ids same year??
-- No duplicate records
select a.data_source, count(*)
from data_warehouse.dim_uth_claim_id a
join data_warehouse.dim_uth_claim_id b on a.member_id_src = b.member_id_src and a.claim_id_src = b.claim_id_src and a.data_source = b.data_source and a.data_year = b.data_year and a.uth_claim_id !=b.uth_claim_id 
group by 1;

-- Claims with multiple uth_claim_ids spanning years ???
-- Yes, many
create table dev.dupe_trvm_claimids as
select a.*, b.data_year as other_year
from dw_qa.dim_uth_claim_id a
join dw_qa.dim_uth_claim_id b on a.member_id_src = b.member_id_src and a.claim_id_src = b.claim_id_src and a.data_source = b.data_source and a.data_year != b.data_year 
group by 1;

select *
from dev.dupe_trvm_claimids
order by member_id_src, data_year 
limit 10;

select *
from dw_qa.dim_uth_member_id dumi 
where member_id_src in (select distinct member_id_src from dev.dupe_trvm_claimids)
order by member_id_src 

--Specific counts for a given optum data_source 
select count(distinct a.data_source || a.member_id_src || a.claim_id_src)
from data_warehouse.dim_uth_claim_id a
join data_warehouse.dim_uth_claim_id b on a.member_id_src = b.member_id_src and a.claim_id_src = b.claim_id_src and a.data_source = b.data_source
left outer join optum_zip.medical m on a.claim_id_src = trim(m.clmid) and a.member_id_src=m.patid::text
where a.data_year = 2016 and b.data_year = 2017 and a.data_source = 'optz' and m.clmid is not null
order by a.member_id_src
limit 10;


--Truven Case
select *
from data_warehouse.dim_uth_claim_id
where claim_id_src ='755.0' and member_id_src ='2840057605.0'
order by member_id_src;

--Optum Dod Case

select *
from data_warehouse.dim_uth_claim_id
where claim_id_src='2417639193' and member_id_src='33003559895'
order by member_id_src;

select *
from dev2016.optum_zip_medical m 
where trim(m.clmid)='2417639193' and m.patid::text='33003559895';

select *
from optum_zip.medical m 
where trim(m.clmid)='2417639193' and m.patid::text='33003559895';

select *
from optum_zip.medical m 
where trim(m.clmid)='J883V3L3J' and m.patid=560499898927193;

select a.patid, a.clmid
from dev2016.optum_zip_medical a
join optum_zip.medical b on trim(a.clmid)=trim(b.clmid) and a.patid=b.patid
where b.patid is not null;
limit 5;

--Scratch
select data_source, data_year, count(*)
from data_warehouse.dim_uth_claim_id
group by 1, 2
order by 1, 2;