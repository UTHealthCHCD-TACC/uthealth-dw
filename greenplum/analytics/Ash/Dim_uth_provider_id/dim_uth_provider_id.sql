
drop table if exists  dev.am_claim_detail;
   
create table dev.am_claim_detail (like data_warehouse.claim_detail);

insert into dev.am_claim_detail 
	select * 
		from data_warehouse.claim_detail;


------------------------------------------------------------------------------------------
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

------------------------------------------------------------------------------------------
--Optum Zip
------------------------------------------------------------------------------------------
--create uth_provider_id

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
--reset values			
update dev.am_claim_detail c
		set bill_provider = null
	where data_source = 'optz';

vacuum analyze dev.am_claim_detail;

--update bill_provider in claim detail	
update dev.am_claim_detail c
		set bill_provider = up.uth_provider_id 
	from data_warehouse.dim_uth_claim_id d, optum_zip.medical m, optum_zip.provider_bridge pb , dev.am_dim_uth_provider_id up 
	where c."year" = d.data_year 
		and c.uth_claim_id = d.uth_claim_id 
		and c.uth_member_id = d.uth_member_id 
		
		and m.clmid::text = c.claim_id_src 
		and m.patid::text = c.member_id_src		
		and m.clmseq = c.claim_sequence_number_src 
		and m.bill_prov = pb.prov 
		
		and pb.prov_unique::text = up.provider_id_src 		
		and d.data_source = 'optz'
		--and d.data_year = 2017		
		
		
vacuum analyze dev.am_claim_detail;
		



select c.year, patid , clmid , clmseq , pb.prov_unique , m.bill_prov, up.uth_provider_id --, m.prov , refer_prov , service_prov 
	from dev.am_claim_detail c, data_warehouse.dim_uth_claim_id d, optum_zip.medical m, optum_zip.provider_bridge pb , dev.am_dim_uth_provider_id up 
	where c."year" = d.data_year 
		and c.uth_claim_id = d.uth_claim_id 
		and c.uth_member_id = d.uth_member_id 
		and m.clmid::text = c.claim_id_src 
		and m.patid::text = c.member_id_src		
		and m.clmseq = c.claim_sequence_number_src 
		and m.bill_prov = pb.prov 
		and pb.prov_unique::text = up.provider_id_src 		
		and d.data_source = 'optz'
		and d.data_year = 2017	


 


			
select year, patid , clmid , clmseq , pb.prov_unique , m.bill_prov, u.uth_provider_id --, m.prov , refer_prov , service_prov 
	from optum_zip.medical m , optum_zip.provider_bridge pb , dev.am_dim_uth_provider_id u, data_warehouse.dim_uth_claim_id c
	where m.bill_prov = pb.prov 
		and pb.prov_unique::text = u.provider_id_src 
		and c.claim_id_src = m.clmid 
		and c.member_id_src = m.patid::text
		

select *
	from dev.am_claim_detail c, data_warehouse.dim_uth_claim_id u, optum_zip.medical m 
	where c.uth_claim_id = u.uth_claim_id 
		and m.clmid::text = u.claim_id_src 
		and m.patid::text = u.member_id_src 
		
		
update dev.am_claim_detail c
		set bill_provider = u.uth_provider_id 
	from data_warehouse.dim_uth_claim_id d, optum_zip.medical m, optum_zip.provider_bridge pb , dev.am_dim_uth_provider_id u 
	where c.uth_claim_id = d.uth_claim_id 
		and m.clmid::text = d.claim_id_src 
		and m.patid::text = d.member_id_src 
		and m.clmseq = c.claim_sequence_number_src 
		and m."year" = 2017
		and d.data_source = 'optz'
		and d.data_year = 2017		
		and m.bill_prov = pb.prov 
		and pb.prov_unique::text = u.provider_id_src 		
		
		
select count(claim_id_src ), claim_id_src, count(claim_sequence_number_src) , claim_sequence_number_src
	from data_warehouse.claim_detail d
	where data_source ='optz'
		and year = 2017
	group by claim_id_src , claim_sequence_number_src 
	having count(claim_id_src ) > 1
		and count(claim_sequence_number_src) > 1
	
	
select *
	from data_warehouse.claim_detail d
	where claim_id_src = 'JVJ9O3JOO'
		and data_source = 'optz'
		and year = 2017
	
	
		 
			
select * from optum_zip.provider_bridge pb where pb.prov_unique = 28888888882 and pb.prov = 8888888882
select * from optum_zip.provider p where p.prov_unique = 28888888882
select * from dev.am_provider ap where ap.uth_provider_id = 104537029
			
			
			
			
			
			
			
			
			
			

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

