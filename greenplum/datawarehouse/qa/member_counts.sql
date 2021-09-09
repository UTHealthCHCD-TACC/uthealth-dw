-- QA

-- Records counts

--DROP TABLE dw_qa.member_counts;
--
CREATE TABLE dw_qa.member_counts (
	data_source varchar NULL,
	"year" int2 null,
	member_count int4 NULL,
	dw_count int4 NULL
)
DISTRIBUTED RANDOMLY;

--delete from dw_qa.member_counts;

-- Optum

-- Claim headers by year in source schema

--select distinct count(*) from optum_zip.medical;

--- OPTUM ---
insert into dw_qa.member_counts 
select 'Optum ZIP' as "source", import."year", import."import_count", dw."dw_count"--, import."import_count" - dw."dw_count" as "difference"
from (
	select count(m.patid) as "import_count", extract(year from generate_series(m.eligeff, m.eligend, interval '1 year')) as "year"
	from optum_zip.mbr_enroll m 
	group by "year") import
join (
	select count(cey.uth_member_id) as "dw_count", cey."year" 
	from data_warehouse.member_enrollment_yearly cey
	where data_source = 'optz'
	group by "year") dw on dw."year" = import."year"

UNION
	
select 'Optum DOD' as "source", import."year", import."import_count", dw."dw_count"--, import."import_count" - dw."dw_count" as "difference"
from (
	select count(m.patid) as "import_count", extract(year from generate_series(m.eligeff, m.eligend, interval '1 year')) as "year"
	from optum_zip.mbr_enroll_r m 
	group by "year") import
join (
	select count(cey.uth_member_id) as "dw_count", cey."year" 
	from data_warehouse.member_enrollment_yearly cey
	where data_source = 'optd'
	group by "year") dw on dw."year" = import."year";

--- TRUVEN ---
insert into dw_qa.member_counts 
select 'Truven' as "source", import."year", import."import_count", dw."dw_count"--, import."import_count" - dw."dw_count" as "difference"
from (
	select sum("import_count") as "import_count", "year" from (
		
		select count(co.msclmid) as "import_count", extract(year from (min(co.svcdate))) as "year"
		from truven.ccaeo co -- comercial outpatient
		group by "year"
		
		union
		
		select count(mo.msclmid) as "import_count", extract(year from (min(mo.svcdate))) as "year"
		from truven.mdcro mo -- medicare outpatient
		group by "year"
		
		union
		
		select count(ci.msclmid) as "import_count", extract(year from (min(ci.svcdate))) as "year"
		from truven.ccaes ci -- commercial inpatient
		group by "year" 
		
		union 
		
		select count(mi.msclmid) as "import_count", extract(year from (min(mi.svcdate))) as "year"
		from truven.mdcrs mi -- medicare inpatient
		group by "year"
		) sq
	group by "year") import
join (
	select count(ch.uth_member_id) as "dw_count", ch."year" 
		from data_warehouse.member_header ch 
		where data_source = 'truv'
		group by "year") dw on dw."year" = import."year";

	
-- Medicare
insert into dw_qa.member_counts 
select 'Medicare' as "source", import."year", import."import_count", dw."dw_count"--, import."import_count" - dw."dw_count" as "difference"
from (
	select sum("import_count") as "import_count", "year" from (
		
		select count(bc.clm_id) as "import_count", min(extract(year from bc.clm_from_dt::date)) as "year"
		from medicare_national.bcarrier_claims_k bc -- B Carrier
		group by "year"
		
		union
		
		select count(dme.clm_id) as "import_count", min(extract(year from dme.clm_from_dt::date)) as "year"
		from medicare_national.dme_claims_k dme -- DME
		group by "year"
		
		union
		
		select count(hha.clm_id) as "import_count", min(extract(year from hha.clm_from_dt::date)) as "year"
		from medicare_national.hha_base_claims_k hha -- HHA
		group by "year"
		
		union
		
		select count(hosp.clm_id) as "import_count", min(extract(year from hosp.clm_from_dt::date)) as "year"
		from medicare_national.hospice_base_claims_k hosp -- Hospice
		group by "year"
		
		union
		
		select count(ip.clm_id) as "import_count", min(extract(year from ip.clm_from_dt::date)) as "year"
		from medicare_national.inpatient_base_claims_k ip -- Inpatient
		group by "year"
		
		union
			
		select count(op.clm_id) as "import_count", min(extract(year from op.clm_from_dt::date)) as "year"
		from medicare_national.outpatient_base_claims_k op -- Outpatient
		group by "year"
		
		union
		
		select count(snf.clm_id) as "import_count", min(extract(year from snf.clm_from_dt::date)) as "year"
		from medicare_national.snf_base_claims_k snf -- SNF
		group by "year"
		) sq
	group by "year") import
join (
	select count(ch.uth_member_id) as "dw_count", ch."year" 
		from data_warehouse.member_header ch 
		where data_source = 'mcrn'
		group by "year") dw on dw."year" = import."year";	
	
	




-- Display counts

select data_source, "year", import_count, dw_count, import_count - dw_count as "difference"
from dw_qa.member_counts cc
order by "data_source", "year";
