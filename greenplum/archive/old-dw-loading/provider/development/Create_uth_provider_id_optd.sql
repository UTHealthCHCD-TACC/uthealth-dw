------------------------------------------------------------------------------------------
--Optum DOD
------------------------------------------------------------------------------------------
--add records to dim_uth_provider_id
insert into data_warehouse.dim_uth_provider_id (data_source, provider_id_src)    
select distinct 'optd' data_source, p.prov_unique::text 
	from optum_dod.provider p
	left join data_warehouse.dim_uth_provider_id d on d.provider_id_src = p.prov_unique::text and d.data_source = 'optd'
	where d.uth_provider_id is null
		and p.prov_unique is not null
		and p.prov_unique > 0;
		
vacuum analyze data_warehouse.dim_uth_provider_id;

--select count(*) from data_warehouse.dim_uth_provider_id 
--select * from data_warehouse.dim_uth_provider_id where data_source = 'optd'
--9719111
-----------------------------------------------------------------------------------------
--add to provider table
insert into data_warehouse.provider (data_source, uth_provider_id, provider_id_src, npi, taxonomy1, taxonomy2, provider_type, state)
	select distinct 'optd' as data_source, u.uth_provider_id, u.provider_id_src , pb.npi, p.taxonomy1 , p.taxonomy2 , 
			p.prov_type::int4, p.prov_state as state
		from optum_dod.provider p , optum_dod.provider_bridge pb, data_warehouse.dim_uth_provider_id u
		left join data_warehouse.provider p2 on p2.uth_provider_id = u.uth_provider_id 
		where p.prov_unique = pb.prov_unique 
			and u.provider_id_src = pb.prov_unique::text
			and p2.uth_provider_id is null;		
			
--select * from data_warehouse.provider ap where ap.data_source = 'optd'
-----------------------------------------------------------------------------------------	
-----------------------------------------------------------------------------------------
--to verify the result: count of resulting table should be equal to optum_zip.medical
		
--replace prov to prov_unique		
drop table if exists data_warehouse.claim_detail_provider_src1;	
--following select statement may never complete in query status window. check user activity for status information.
select 'optd' as data_source, m."year" as data_year, 
		m.patid::text as member_id_src, m.clmid::text as claim_id_src, m.clmseq as claim_sequence_number, m.fst_dt as from_date_of_service, 
		pb_bill.prov_unique as bill_provider,
		pb_refer.prov_unique as ref_provider,
		pb_service.prov_unique as other_provider,
		pb_rendering.prov_unique as perf_rn_provider
	into data_warehouse.claim_detail_provider_src1
	from optum_dod.medical m 
	left outer join optum_dod.provider_bridge pb_bill on m.bill_prov = pb_bill.prov 	
	left outer join optum_dod.provider_bridge pb_refer on m.refer_prov = pb_refer.prov 		
	left outer join optum_dod.provider_bridge pb_service on m.service_prov = pb_service.prov 		
	left outer join optum_dod.provider_bridge pb_rendering on m.prov = pb_rendering.prov ;
	
select * 
	into data_warehouse.dim_uth_provider_id_optd
	from data_warehouse.dim_uth_provider_id d
	where d.data_source = 'optd'
	
--replace prov_unique to uth_provider_id
drop table if exists data_warehouse.claim_detail_provider_src2;	
--following select statement may never complete in query status window. check user activity for status information.
select src.data_source , data_year , member_id_src , claim_id_src , claim_sequence_number , from_date_of_service , 
		ut_bill.uth_provider_id as bill_provider , 
		ut_refer.uth_provider_id as ref_provider,
		ut_service.uth_provider_id as other_provider,
		ut_rendering.uth_provider_id as perf_rn_provider
	into data_warehouse.claim_detail_provider_src2
	from data_warehouse.claim_detail_provider_src1 src
	left outer join data_warehouse.dim_uth_provider_id_optd ut_bill on ut_bill.provider_id_src::bigint = src.bill_provider 
	left outer join data_warehouse.dim_uth_provider_id_optd ut_refer on ut_refer.provider_id_src::bigint = src.ref_provider 
 	left outer join data_warehouse.dim_uth_provider_id_optd ut_service on ut_service.provider_id_src::bigint = src.other_provider 
 	left outer join data_warehouse.dim_uth_provider_id_optd ut_rendering on ut_rendering.provider_id_src::bigint = src.perf_rn_provider; 
	 
 
 --delete old data
 delete from data_warehouse.claim_detail_provider
 	where data_source = 'optd'

 --replace member_id_src and claim_id_src to uth_member_id and uth_claim_id respectively
 insert into data_warehouse.claim_detail_provider(data_source , data_year , uth_member_id , uth_claim_id , claim_sequence_number , from_date_of_service ,
								 		  bill_provider , ref_provider , other_provider , perf_rn_provider)
 select s.data_source , s.data_year , d.uth_member_id , d.uth_claim_id , s.claim_sequence_number , s.from_date_of_service ,
 		s.bill_provider , s.ref_provider , s.other_provider , s.perf_rn_provider
 	from data_warehouse.claim_detail_provider_src2 s
 	inner join data_warehouse.dim_uth_claim_id d on d.claim_id_src = s.claim_id_src 
 												and d.member_id_src = s.member_id_src 
 												and d.data_year = s.data_year 
	where d.data_source = 'optd'												
 
 
 select count(*) from data_warehouse.claim_detail_provider
 where data_source = 'optd'
 	 