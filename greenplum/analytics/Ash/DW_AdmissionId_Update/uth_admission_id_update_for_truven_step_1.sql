 /*
 drop table dev.am_claim_header
   
 create table dev.am_claim_header (like data_warehouse.claim_header)

 insert into dev.am_claim_header 
	select * 
		from data_warehouse.claim_header
*/		
select count(*) from dev.am_claim_header ach where uth_admission_id is not null and data_source in ( 'truv')

--reset existing data; if any; 
/*
 update dev.am_claim_header
	set uth_admission_id = null
	where data_source in ('truv')
*/
 		
update dev.am_claim_header h
		set uth_admission_id = a.uth_admission_id 
	from data_warehouse.dim_uth_admission_id a
	where h.uth_member_id = a.uth_member_id 
		and h.data_source = a.data_source 
		and h.data_year = a."year" 
		and h.admission_id_src = a.admission_id_src 
		and h.admission_id_src is not null
		and h.data_source = 'truv'		
		
--------------------------------------------------------------------------------------------
--update TRUV from ccaei, ccaef, mdcrf, mdcri
--CCAEF
insert into dw_qa.admission_header (data_source , year , uth_admission_id , uth_member_id , admission_id_src , member_id_src , table_id_src )	
	select distinct data_source , 
			duai."year" , 
			uth_admission_id , 
			uth_member_id , 			
			duai.admission_id_src, 
			duai. member_id_src,
			'dim_uth_admission_id , truven.ccaef' as table_id_src		
		from data_warehouse.dim_uth_admission_id duai 
		inner join truven.ccaef c on cast(c.caseid as text) = duai.admission_id_src 
											and cast(c.enrolid as text) = duai.member_id_src 
											and c."year" = duai."year" 	
		where data_source = 'truv'		
			and duai.uth_admission_id not in (select uth_admission_id 
												from dw_qa.admission_header 
												where data_source = 'truv')	
												
												
--CCAEI		
insert into dw_qa.admission_header (data_source , year , uth_admission_id , uth_member_id , admission_id_src , member_id_src , table_id_src )	
		select distinct data_source , 
			duai."year" , 
			uth_admission_id , 
			uth_member_id , 			
			duai.admission_id_src, 
			duai. member_id_src,
			'dim_uth_admission_id , truven.ccaei' as table_id_src				
		from data_warehouse.dim_uth_admission_id duai 
		inner join truven.ccaei c on cast(c.caseid as text) = duai.admission_id_src 
											and cast(c.enrolid as text) = duai.member_id_src 
											and c."year" = duai."year" 	
		where data_source = 'truv'	
			and duai.uth_admission_id not in (select uth_admission_id 
												from dw_qa.admission_header 
												where data_source = 'truv')													
												
--MDCRF											
 insert into dw_qa.admission_header (data_source , year , uth_admission_id , uth_member_id , admission_id_src , member_id_src , table_id_src )	
	select distinct data_source , 
			duai."year" , 
			uth_admission_id , 
			uth_member_id , 			
			duai.admission_id_src, 
			duai. member_id_src,
			'dim_uth_admission_id , truven.ccaef' as table_id_src			
		from data_warehouse.dim_uth_admission_id duai 
		inner join truven.mdcrf c on cast(c.caseid as text) = duai.admission_id_src 
											and cast(c.enrolid as text) = duai.member_id_src 
											and c."year" = duai."year" 	
		where data_source = 'truv'	
			and duai.uth_admission_id not in (select uth_admission_id 
												from dw_qa.admission_header 
												where data_source = 'truv')													
												
--MDCRI													
insert into dw_qa.admission_header (data_source , year , uth_admission_id , uth_member_id , admission_id_src , member_id_src , table_id_src )		
		select distinct data_source , 
			duai."year" , 
			uth_admission_id , 
			uth_member_id , 			
			duai.admission_id_src, 
			duai. member_id_src,
			'dim_uth_admission_id , truven.mdcri' as table_id_src		
		from data_warehouse.dim_uth_admission_id duai 
		inner join truven.mdcri c on cast(c.caseid as text) = duai.admission_id_src 
											and cast(c.enrolid as text) = duai.member_id_src 
											and c."year" = duai."year" 	
		where data_source = 'truv'	
			and duai.uth_admission_id not in (select uth_admission_id 
												from dw_qa.admission_header 
												where data_source = 'truv')					
												
												
--------------------------------------------------------------------------------------------														
												
	--check for duplicates											
	select uth_admission_id , count(uth_admission_id )
		from dw_qa.admission_header
		where data_source = 'truv'	
		group  by uth_admission_id , "year" 
		having count (uth_admission_id ) > 1
		
	 
												
												
												
												
												
												
												
												
												
												

	

											
	
 
											

 
												
	
		
		
		
	 