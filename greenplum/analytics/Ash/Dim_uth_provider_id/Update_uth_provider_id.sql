--create dim_uth_provider_id
drop table if exists dev.am_dim_uth_provider_id;

CREATE TABLE dev.am_dim_uth_provider_id (
	uth_provider_id bigserial NOT NULL,	
	data_source bpchar(4) NULL,
	provider_id_src text NOT NULL
)
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (uth_provider_id);

alter sequence dev.am_dim_uth_provider_id_uth_provider_id_seq restart with 100000000;

alter sequence dev.am_dim_uth_provider_id_uth_provider_id_seq cache 200;

analyze dev.am_dim_uth_provider_id;

--create provier table








------------------------------------------------------------------------------------------
--Optum Zip
------------------------------------------------------------------------------------------
--add records to dim_uth_provider_id
insert into dev.am_dim_uth_provider_id (data_source, provider_id_src)    
select distinct 'optz' data_source, p.prov_unique::text 
	from optum_zip.provider p
	left join dev.am_dim_uth_provider_id d on d.provider_id_src = p.prov_unique::text and d.data_source = 'optz'
	where d.uth_provider_id is null
		and p.prov_unique is not null;
		
vacuum analyze dev.am_dim_uth_provider_id;

--select count(*) from dev.am_dim_uth_provider_id 
--select * from dev.am_dim_uth_provider_id
--9719112
-----------------------------------------------------------------------------------------
--add to provider table
insert into dev.am_provider (data_source, uth_provider_id, provider_id_src, npi, taxonomy1, taxonomy2, provider_type, state)
	select distinct 'optz' as data_source, u.uth_provider_id, u.provider_id_src , pb.npi, p.taxonomy1 , p.taxonomy2 , 
			p.prov_type::int4, p.prov_state as state
		from optum_zip.provider p , optum_zip.provider_bridge pb, dev.am_dim_uth_provider_id u
		left join dev.am_provider p2 on p2.uth_provider_id = u.uth_provider_id 
		where p.prov_unique = pb.prov_unique 
			and u.provider_id_src = pb.prov_unique::text
			and p2.uth_provider_id is null		
		
--select * from dev.am_provider ap 
-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------
drop table if exists  dev.am_claim_detail;
create table dev.am_claim_detail (like dev.am_claim_detail_staging);

-----------------------------------------------------------------------------------------
--start
drop table if exists dev.am_claim_detail_target_year;
select 2008 as target_year
	into dev.am_claim_detail_target_year;

-----------------------------------------------------------------------------------------
drop table if exists  dev.am_claim_detail_staging;		
create table dev.am_claim_detail_staging (like data_warehouse.claim_detail);
insert into dev.am_claim_detail_staging 
	select distinct d.* 
		from data_warehouse.claim_detail d, dev.am_claim_detail_target_year ty
		where data_source = 'optz'
			and "year" = ty.target_year;	
		
ALTER TABLE dev.am_claim_detail_staging ADD bill_provider numeric NULL;
ALTER TABLE dev.am_claim_detail_staging ADD ref_provider numeric NULL;
ALTER TABLE dev.am_claim_detail_staging ADD perf_rn_provider numeric NULL;
ALTER TABLE dev.am_claim_detail_staging ADD perf_at_provider numeric NULL;
ALTER TABLE dev.am_claim_detail_staging ADD perf_op_provider numeric NULL;
ALTER TABLE dev.am_claim_detail_staging ADD other_provider numeric NULL;

--select * from dev.am_claim_detail_staging
-----------------------------------------------------------------------------------------	
--update billing provider
drop table if exists  dev.am_provider_src;	
select distinct m."year"::text , m.clmid::text as claim_id_src, m.patid::text as member_id_src, 
		m.clmseq::text as claim_sequence_number_src, m.bill_prov as provider_id_src, up.uth_provider_id 
	into dev.am_provider_src
	from optum_zip.medical m, optum_zip.provider_bridge pb , dev.am_dim_uth_provider_id up, dev.am_claim_detail_target_year ty 
	where m."year" = ty.target_year
		and m.bill_prov = pb.prov
		and pb.prov_unique::text = up.provider_id_src ;				
	
update dev.am_claim_detail_staging c
		set bill_provider = p.uth_provider_id 
	from dev.am_provider_src p
	where c."year"::text = p."year"::text
		and c.claim_id_src::text = p.claim_id_src::text
		and c.member_id_src::text = p.member_id_src::text
		and c.claim_sequence_number_src::text  = p.claim_sequence_number_src::text
		and c.bill_provider_id::text = p.provider_id_src::text; 
 
--update referring provider
drop table if exists  dev.am_provider_src;	
select distinct m."year"::text , m.clmid::text as claim_id_src, m.patid::text as member_id_src, 
		m.clmseq::text as claim_sequence_number_src, m.refer_prov as provider_id_src, up.uth_provider_id 
	into dev.am_provider_src
	from optum_zip.medical m, optum_zip.provider_bridge pb , dev.am_dim_uth_provider_id up, dev.am_claim_detail_target_year ty  
	where m."year" = ty.target_year
		and m.refer_prov = pb.prov
		and pb.prov_unique::text = up.provider_id_src;			
		
update dev.am_claim_detail_staging c
		set ref_provider = p.uth_provider_id 
	from dev.am_provider_src p
	where c."year"::text = p."year"::text
		and c.claim_id_src::text = p.claim_id_src::text
		and c.member_id_src::text = p.member_id_src::text
		and c.claim_sequence_number_src::text  = p.claim_sequence_number_src::text
		and c.ref_provider_id::text = p.provider_id_src::text; 		
		
--update rendering provider		
drop table if exists  dev.am_provider_src;	
select distinct m."year"::text , m.clmid::text as claim_id_src, m.patid::text as member_id_src, 
		m.clmseq::text as claim_sequence_number_src, m.prov as provider_id_src, up.uth_provider_id 
	into dev.am_provider_src
	from optum_zip.medical m, optum_zip.provider_bridge pb , dev.am_dim_uth_provider_id up, dev.am_claim_detail_target_year ty  
	where m."year" = ty.target_year
		and m.prov = pb.prov
		and pb.prov_unique::text = up.provider_id_src;			
				
update dev.am_claim_detail_staging c
		set perf_rn_provider = p.uth_provider_id 
	from dev.am_provider_src p 
	where c."year"::text = p."year"::text
		and c.claim_id_src::text = p.claim_id_src::text
		and c.member_id_src::text = p.member_id_src::text
		and c.claim_sequence_number_src::text  = p.claim_sequence_number_src::text
		and c.perf_provider_id::text = p.provider_id_src::text; 							
		
--update service provider
drop table if exists  dev.am_provider_src;	
select distinct m."year"::text , m.clmid::text as claim_id_src, m.patid::text as member_id_src, 
		m.clmseq::text as claim_sequence_number_src, m.charge, m.proc_cd , m.fst_dt ,
		m.service_prov as provider_id_src, up.uth_provider_id 
	into dev.am_provider_src
	from optum_zip.medical m, optum_zip.provider_bridge pb , dev.am_dim_uth_provider_id up, dev.am_claim_detail_target_year ty  
	where m."year" = ty.target_year
		and m.service_prov > 0
		and m.service_prov = pb.prov
		and pb.prov_unique::text = up.provider_id_src;			
		
	
update dev.am_claim_detail_staging c
		set other_provider = p.uth_provider_id 
	from dev.am_provider_src p  
	where c."year"::text = p."year"::text
		and c.claim_id_src::text = p.claim_id_src::text
		and c.member_id_src::text = p.member_id_src::text
		and c.claim_sequence_number_src::text  = p.claim_sequence_number_src::text	
		and c.charge_amount = p.charge
		and c.cpt_hcpcs = p.proc_cd
		and c.from_date_of_service = p.fst_dt;
		
-----------------------------------------------------------------------------------------	
insert into dev.am_claim_detail
select * 
	from dev.am_claim_detail_staging
-----------------------------------------------------------------------------------------	
	
		
--vacuum analyze dev.am_claim_detail_2007;
 

-----------------------------------------------------------------------------------------
 select distinct year from dev.am_claim_detail acd 
-----------------------------------------------------------------------------------------




		
		
		
		

 



--drop the columns after updating all the datasources
--ALTER TABLE dev.am_claim_detail DROP COLUMN perf_provider_id;
--ALTER TABLE dev.am_claim_detail DROP COLUMN bill_provider_id;
--ALTER TABLE dev.am_claim_detail DROP COLUMN ref_provider_id;


 

------------------------------------------------------------------------------------------




--update bill_provider in claim detail	
	
		
	

 
			

/*
 --debug 
 
 
 select c1.clm_id ,
		c1.org_npi_num , c1.at_physn_npi , c1.op_physn_npi , c1.ot_physn_npi , c1.rndrng_physn_npi 
	from medicare_national.inpatient_base_claims_k c1, medicare_national.inpatient_base_claims_k c2
	where c1.clm_id = c2.clm_id 
		and c1.org_npi_num <> c2.at_physn_npi 
		and c1.org_npi_num <> c2.op_physn_npi 
		and c1.org_npi_num <> c2.ot_physn_npi 
		and c1.org_npi_num <> c2.rndrng_physn_npi		
		and c1.at_physn_npi <> c2.op_physn_npi 
		and c1.at_physn_npi <> c2.ot_physn_npi 
		and c1.at_physn_npi <> c2.rndrng_physn_npi		
		and c1.op_physn_npi <> c2.ot_physn_npi 
		and c1.op_physn_npi <> c2.rndrng_physn_npi		
		and c1.ot_physn_npi <> c2.rndrng_physn_npi	
		and c1."year" = '2017'
		and c2."year" = '2017'
		and c1.org_npi_num is not null		
		and c1.at_physn_npi is not null		
		and c1.op_physn_npi is not null		
		and c1.ot_physn_npi is not null		
		and c1.rndrng_physn_npi is not null		
		 
		
select *
	from optum_zip.medical m 
	where "year" = 2017
	order by clmid, clmseq 
	
	
	
	
select distinct  m1.clmid , m1.clmseq , m1.bill_prov , m1.refer_prov , m1.prov , m1.service_prov 
	from optum_zip.medical m1, optum_zip.medical m2  
	where m1."year" = 2017
		and m2."year" = 2017
		and m1.clmid = m2.clmid 
		and (m1.prov <> m2.prov) 
	order by m1.clmid, m1.clmseq 
	
 
 
 
 
 */

