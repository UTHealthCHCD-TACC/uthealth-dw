drop table if exists dev.am_mbp_members;

select distinct y.uth_member_id , m.member_id_src  
	into dev.am_mbp_members
	from data_warehouse.member_enrollment_yearly y, data_warehouse.dim_uth_member_id m
	where y.uth_member_id = m.uth_member_id 
		and y.data_source = 'optz'
		and age_derived between 9 and 18		
		and year between 2011 and 2018;
		
------------------------------------------------------------------------------------------	
drop table if exists dev.am_mbp_members_continuously_enrolled;

select distinct y.uth_member_id , m.member_id_src  
	into dev.am_mbp_members_continuously_enrolled
	from data_warehouse.member_enrollment_yearly y, dev.am_mbp_members m
	where y.uth_member_id  = m.uth_member_id
		and y.data_source = 'optz'
		and y.total_enrolled_months = 12
		and year between 2011 and 2018;
------------------------------------------------------------------------------------------			
drop table if exists dev.am_mbp_members_with_cpt;

select distinct d.uth_member_id , m.member_id_src 
	into dev.am_mbp_members_with_cpt
	from data_warehouse.claim_detail d, dev.am_mbp_members_continuously_enrolled m
	where d.uth_member_id = m.uth_member_id
		and d.data_source = 'optz'
		and d."year" in (2011,2012,2013,2014,2015,2016,2017,2018)
		and d.cpt_hcpcs in ('90702','90714','90715','90718');

------------------------------------------------------------------------------------------
--get member enrollment from optum_zip
drop table if exists dev.am_mbp_enrollment;

select distinct patid , eligeff , eligend , gdr_cd , yrdob , zipcode_5  
	into dev.am_mbp_enrollment
	from optum_zip.mbr_enroll e, dev.am_mbp_members_with_cpt m
	where e.patid::text = m.member_id_src		
		and (date_part('year', eligeff) between 2011 and 2018 
			 or 
			 date_part('year', eligend) between 2011 and 2018);
------------------------------------------------------------------------------------------	
--get medical from optum_zip
drop table if exists dev.am_mbp_medical;	

select distinct c.patid , admit_type , bill_prov , charge , clmid , clmseq , coins , conf_id , copay , deduct ,
		fst_dt , icd_flag , loc_cd , ndc , paid_dt , paid_status , pos , proc_cd , procmod , prov , prov_par ,
		provcat , refer_prov , rvnu_cd , service_prov , std_cost , std_cost_yr , tos_cd 
	into dev.am_mbp_medical
	from optum_zip.medical c , dev.am_mbp_members_with_cpt m
	where c.patid::text = m.member_id_src
		and "year" between 2011 and 2018; 
		
------------------------------------------------------------------------------------------	
--get provider from optum_zip		
drop table if exists dev.am_mbp_provider;			
	
select distinct p.prov_unique , p.prov_state , p.prov_type , p.provcat , pb.prov 
	into dev.am_mbp_provider
	from dev.am_mbp_medical m, optum_zip.provider p , optum_zip.provider_bridge pb 
	where m.prov = pb.prov 
		and pb.prov_unique = p.prov_unique ;


------------------------------------------------------------------------------------------	
--clean up
drop table if exists dev.am_mbp_members;
drop table if exists dev.am_mbp_members_continuously_enrolled;
drop table if exists dev.am_mbp_members_with_cpt;


		
	