
drop table if exists  dev.am_optz_claim_detail;
   
create table dev.am_optz_claim_detail (like data_warehouse.claim_detail);

ALTER TABLE dev.am_optz_claim_detail DROP COLUMN perf_provider_id;
ALTER TABLE dev.am_optz_claim_detail DROP COLUMN bill_provider_id;
ALTER TABLE dev.am_optz_claim_detail DROP COLUMN ref_provider_id;

ALTER TABLE dev.am_optz_claim_detail ADD bill_provider numeric NULL;
ALTER TABLE dev.am_optz_claim_detail ADD ref_provider numeric NULL;
ALTER TABLE dev.am_optz_claim_detail ADD perf_rn_provider numeric NULL;
ALTER TABLE dev.am_optz_claim_detail ADD perf_at_provider numeric NULL;
ALTER TABLE dev.am_optz_claim_detail ADD perf_op_provider numeric NULL;
ALTER TABLE dev.am_optz_claim_detail ADD other_provider numeric NULL;

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

insert into data_warehouse.claim_detail(
	data_source, year, 
	-- NEW
	year_adj,
	-- END NEW
	uth_claim_id, uth_member_id,
    claim_sequence_number, claim_sequence_number_src,
	from_date_of_service, to_date_of_service, month_year_id,		
	network_ind, network_paid_ind,
	admit_date,	discharge_date,
	procedure_cd, procedure_type, proc_mod_1, proc_mod_2,
	revenue_cd, charge_amount, allowed_amount, paid_amount, 
	-- NEW
	charge_amount_adj, allowed_amount_adj, paid_amount_adj,
	-- END NEW
	copay, deductible, coins, cob, cob_type,
	bill_type_inst,	bill_type_class, bill_type_freq, units,
	drg_cd,
	claim_id_src, member_id_src, table_id_src, data_year)
	
	
	
	
	
select uth.data_source, uth.data_year, 
	-- NEW
	m.std_cost_yr::int,
	-- END NEW
	uth.uth_claim_id, uth.uth_member_id,
	trunc(m.clmseq::int4), m.clmseq,
	m.fst_dt, m.lst_dt, --get_my_from_date(m.fst_dt),	
	null, null, --No mappings for network fields
	conf.admit_date, conf.disch_date,
	m.proc_cd, null, substring(m.procmod, 1,1), substring(m.procmod, 2,1),
	m.rvnu_cd, 
	m.charge, m.std_cost, null, 
	-- NEW
	(m.charge * cf.cost_factor), (m.std_cost * cf.cost_factor), null,
	-- END NEW
	m.copay, null, m.coins, null, m.cob, --NOTE: cob is an int, but optum is varchar -> m.cob (Find where it is a numeric value, set other to zero), 	--NOTE: Left pad revenu_cd to 4 digits with leading zero
	bt.inst_code, bt.class_code, null, m.units, --NOTE: bill_type_freq is null for optum
	m.drg,
	uth.claim_id_src, uth.member_id_src, 'medical', m.year
from --data_warehouse.claim_header ch join 
data_warehouse.dim_uth_claim_id uth --on ch.uth_member_id = uth.uth_member_id and ch.uth_claim_id=uth.uth_claim_id
join optum_zip.medical m on uth.claim_id_src=m.clmid::text and uth.member_id_src=m.patid::text
-- NEW
join reference_tables.ref_optum_cost_factor cf on cf.service_type = left(m.tos_cd, (position('.' in m.tos_cd)-1)) and cf.standard_price_year = m.std_cost_yr::int
-- END NEW
left outer join optum_zip.confinement conf on m.conf_id=conf.conf_id
left outer join reference_tables.ref_optum_bill_type_from_tos bt on m.tos_cd=bt.tos
--provider start 
left outer join optum_zip.provider_bridge pb on pb.prov = m.bill_prov 
left outer join optum_zip.provider p on p.prov_unique = pb.prov_unique 
--provider end
where uth.data_source='optz' 
			
			
			
			
			
			
			
			
			


			
	 



			
		
		
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

