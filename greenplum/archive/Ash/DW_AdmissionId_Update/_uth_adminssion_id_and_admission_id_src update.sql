/*
  select m.conf_id, m.clmid , m.patid , m."year", duci.uth_claim_id, duci.data_source, duci.uth_member_id, 
		duai.uth_admission_id, duai.admission_id_src 
	from dev.am_optz_medical m 
	left join dev.amdim_uth_claim_id duci on duci.claim_id_src = m.clmid														
														and duci.data_year = m."year" 
														and duci.data_source = 'optz'	
														and duci.member_id_src = cast(m.patid as text)
	left join dev.am_dim_uth_admission_id duai on duai.admission_id_src = m.conf_id 
														and duai."year" = m."year" 
														and duai.data_source = 'optd'	
														and duai.member_id_src = cast(m.patid as text)
	where m.conf_id  is not null  
	
 --------------------------------------------------------------------------------------------------
create table dev.am_claim_header
	with(appendonly=true,orientation=column,compresstype=zlib)
	as select * from data_warehouse.claim_header where data_source = 'optz' and data_year = 2017
	distributed by(uth_member_id) 
---------------------------------------------------------------------------------------------------
*/	
--10,371,001,371
select count(distinct uth_claim_id ) 
	from data_warehouse.dim_uth_claim_id duci 

--Total records 10,157,931,630	
--Distinct 10,021,013,848
select count(distinct uth_claim_id )  
	from data_warehouse.claim_header ch 

/*
10,371,001,371 (total uth_claim_id from key table) - 10,021,013,848 (distinct uth_claim_id count from claim_header) = 349,987,523 ???? lost?


does every claim contains conf_id?
if so, then 1 to 1 match is possible.
*/	

	

select duci.uth_claim_id, duci.uth_member_id, duci.data_source, duci.claim_id_src, duci.member_id_src, duci.data_year,
		ah.uth_admission_id, ah.admission_id_src, ah.total_allowed_amount, ah.total_charge_amount,  
		ch.total_charge_amount, ch.total_allowed_amount 
	from data_warehouse.dim_uth_claim_id duci 
	inner join data_warehouse.claim_header ch on ch.uth_member_id = duci.uth_member_id   
												and ch.uth_claim_id = duci.uth_claim_id
												and ch.data_source = duci.data_source 
												and ch.data_year = duci.data_year 
												and ch.claim_id_src = duci.claim_id_src 
												and ch.member_id_src = duci.member_id_src 
	left join dw_qa.admission_header ah on ah.uth_member_id = duci.uth_member_id 
												and ah.data_source = duci.data_source 
												and ah.member_id_src = duci.member_id_src 
												and ah."year" = duci.data_year 
	where duci.data_source = 'optz'
		and duci.data_year = 2015
		and ah.uth_admission_id is not null 
	order by duci.uth_claim_id, ah.uth_admission_id 
												
	--left join data_warehouse.dim_uth_admission_id duai on duai.uth_member_id = duci.uth_member_id 
	--											and duai.data_source = duci.data_source 
	--											and duai.member_id_src = duci.member_id_src 
	--											and duai."year" = duci.data_year 
	
	
	

 







































