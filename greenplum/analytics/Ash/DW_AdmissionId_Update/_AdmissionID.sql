/* 
  select m.conf_id, m.clmid , m.patid , m."year", duci.uth_claim_id, duci.data_source, duci.uth_member_id, 
		duai.uth_admission_id, duai.admission_id_src 
	from optum_zip.medical m 
	left join data_warehouse.dim_uth_claim_id duci on duci.claim_id_src = m.clmid														
														and duci.data_year = m."year" 
														and duci.data_source = 'optd'	
														and duci.member_id_src = cast(m.patid as text)
	left join data_warehouse.dim_uth_admission_id duai on duai.admission_id_src = m.conf_id 
														and duai."year" = m."year" 
														and duai.data_source = 'optd'	
														and duai.member_id_src = cast(m.patid as text)
	where m.conf_id  is not null  
 
  
  */

--26241208202
select distinct count(*)
	from data_warehouse.dim_uth_admission_id duai 
	inner join data_warehouse.dim_uth_claim_id duci on duci.uth_member_id = duai.uth_member_id 
												   and duci.data_source = duai.data_source 
												   and duci.data_year = duai.year 												   
	inner join data_warehouse.claim_header ch on ch.uth_member_id = duai.uth_member_id 
												   and ch.claim_id_src = duci.claim_id_src 
												   and ch.data_source = duai.data_source 
												   and ch.data_year = duai.year 	

--2276059130												   
select count(distinct ch.uth_claim_id)
	from data_warehouse.dim_uth_admission_id duai 
	inner join data_warehouse.dim_uth_claim_id duci on duci.uth_member_id = duai.uth_member_id 
												   and duci.data_source = duai.data_source 
												   and duci.data_year = duai.year 												   
	inner join data_warehouse.claim_header ch on ch.uth_member_id = duai.uth_member_id 
												   and ch.claim_id_src = duci.claim_id_src 
												   and ch.data_source = duai.data_source 
												   and ch.data_year = duai.year 
		
--10,157,931,630												   
select distinct count(ch.uth_claim_id)
	from data_warehouse.claim_header ch 											   

	
--27197264312	
select distinct ch.data_source, ch.year, ch.uth_claim_id, ch.uth_member_id, ch.from_date_of_service, ch.claim_type, 
		duai.uth_admission_id, duai.admission_id_src, 
		ch.total_charge_amount, ch.total_allowed_amount, ch.total_paid_amount, ch.claim_id_src, ch.member_id_src, 
		ch.table_id_src, ch.bill_type, ch.data_year, ch.total_charge_amount_adj, ch.total_allowed_amount_adj, 
		ch.total_paid_amount_adj, ch.year_adj, ch.to_date_of_service
	from data_warehouse.dim_uth_admission_id duai 
	inner join data_warehouse.dim_uth_claim_id duci on duci.uth_member_id = duai.uth_member_id 
												   and duci.data_source = duai.data_source 
												   and duci.data_year = duai.year 												   
	inner join data_warehouse.claim_header ch on ch.uth_member_id = duai.uth_member_id 
												   and ch.claim_id_src = duci.claim_id_src 
												   and ch.data_source = duai.data_source 
												   and ch.data_year = duai.year 	
		
												   
												   
												   
select distinct ch.data_source, ch.year, ch.uth_claim_id, ch.uth_member_id, ch.from_date_of_service, ch.claim_type, 
		duai.uth_admission_id, duai.admission_id_src, 
		ch.total_charge_amount, ch.total_allowed_amount, ch.total_paid_amount, ch.claim_id_src, ch.member_id_src, 
		ch.table_id_src, ch.bill_type, ch.data_year, ch.total_charge_amount_adj, ch.total_allowed_amount_adj, 
		ch.total_paid_amount_adj, ch.year_adj, ch.to_date_of_service
	from data_warehouse.dim_uth_admission_id duai 
	inner join data_warehouse.dim_uth_claim_id duci on duci.uth_member_id = duai.uth_member_id 
												   and duci.data_source = duai.data_source 
												   and duci.data_year = duai.year 												   
	inner join data_warehouse.claim_header ch on ch.uth_member_id = duai.uth_member_id 
												   and ch.claim_id_src = duci.claim_id_src 
												   and ch.data_source = duai.data_source 
												   and ch.data_year = duai.year 		
	 			   
												   
	-------------------------------------------------------------------------------------------------------------
												   
 select m.conf_id, m.clmid , m.patid , m."year", duci.uth_claim_id, duci.data_source, duci.uth_member_id, 
		duai.uth_admission_id, duai.admission_id_src 
	from optum_zip.medical m 
	left join data_warehouse.dim_uth_claim_id duci on duci.claim_id_src = m.clmid														
														and duci.data_year = m."year" 
														and duci.data_source = 'optd'	
														and duci.member_id_src = cast(m.patid as text)
	left join data_warehouse.dim_uth_admission_id duai on duai.admission_id_src = m.conf_id 
														and duai."year" = m."year" 
														and duai.data_source = 'optd'	
														and duai.member_id_src = cast(m.patid as text)
	where m.conf_id  is not null  			   
												   
								
	
	--same claim id for different patients; different file names and different years
	select patid, clmid, year, conf_id 
		from optum_zip.medical m 
		where conf_id is not null
		--clmid = '202667093'
	    --	and year = 2010
			
	
	
	select * 
		from data_warehouse.dim_uth_admission_id duai 
		where admission_id_src = 'LRLZ4OL64OK46'-- 'MZNNT4LLKM4OO'
		
		
		select uth_admission_id 
			from data_warehouse.dim_uth_admission_id duai 
			inner join optum_zip.medical m on m.conf_id = duai.admission_id_src 
			where cast(m.patid as text) = ch.member_id_src 				 
				and m.clmid = ch.claim_id_src 
				and m.year = ch.data_year 
				and m.conf_id is not null
				and duai.data_source = 'optz'
	
	
	
	select * from data_warehouse.claim_header where data_source = 'optz' and data_year = 2017
	
	
												   
												   
												    
 