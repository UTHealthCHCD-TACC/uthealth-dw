 /*
 drop table dev.am_claim_header
   
 create table dev.am_claim_header (like data_warehouse.claim_header)

 insert into dev.am_claim_header 
	select * 
		from data_warehouse.claim_header
*/		
select count(*) from dev.am_claim_header ach where uth_admission_id is not null and data_source in ('optz','optd','truv')

--reset existing data; if any; 
/*
 update dev.am_claim_header
	set uth_admission_id = null
	where data_source in ('optz','optd')
*/

update dev.am_claim_header h
		set uth_admission_id = a.uth_admission_id 
	from data_warehouse.dim_uth_admission_id a
	where h.uth_member_id = a.uth_member_id 
		and h.data_source = a.data_source 
		and h.data_year = a."year" 
		and h.admission_id_src = a.admission_id_src 
		and h.admission_id_src is not null
		and h.data_source = 'optd'
		 
update dev.am_claim_header h
		set uth_admission_id = a.uth_admission_id 
	from data_warehouse.dim_uth_admission_id a
	where h.uth_member_id = a.uth_member_id 
		and h.data_source = a.data_source 
		and h.data_year = a."year" 
		and h.admission_id_src = a.admission_id_src 
		and h.admission_id_src is not null
		and h.data_source = 'optz'
 
--------------------------------------------------------------------------------------------
--update OPTZ to admission_header in dw_qa (claim level data in dw_qa.admission_header not updated : admit_type, admit_channel, bill_type)
--total_paid_amount = N/A
insert into dw_qa.admission_header (data_source , year , uth_admission_id , uth_member_id , admit_date , discharge_date , discharge_status , primary_diagnosis_cd , 
									primary_icd_proc_cd , total_charge_amount , total_allowed_amount , admission_id_src , member_id_src , table_id_src )
	select distinct data_source , 
			duai."year" , 
			uth_admission_id , 
			uth_member_id , 
			c.admit_date, 
			c.disch_date,
			c.dstatus as discharge_status, 
			c.diag1  as primary_diagnosis_cd, 
			c.proc1  as primary_icd_proc_cd,
			c.charge as total_charge_amount, 
			c.std_cost as total_allowed_amount,		
			duai.admission_id_src, 
			duai. member_id_src,
			'dim_uth_admission_id , optum_zip.confinement' as table_id_src		
		from data_warehouse.dim_uth_admission_id duai 
		inner join optum_zip.confinement c on c.conf_id = duai.admission_id_src 
											and cast(c.patid as text) = duai.member_id_src 
											and c."year" = duai."year" 	
		where data_source = 'optz'
			and duai.uth_admission_id not in (select uth_admission_id 
												from dw_qa.admission_header 
												where data_source = 'optz')	
--------------------------------------------------------------------------------------------
--update OPTD to admission_header in dw_qa (claim level data in dw_qa.admission_header not updated : admit_type, admit_channel, bill_type)
--total_paid_amount = N/A
insert into dw_qa.admission_header (data_source , year , uth_admission_id , uth_member_id , admit_date , discharge_date , discharge_status , primary_diagnosis_cd , 
									primary_icd_proc_cd , total_charge_amount , total_allowed_amount , admission_id_src , member_id_src , table_id_src )
	select distinct data_source , 
			duai."year" , 
			uth_admission_id , 
			uth_member_id , 
			c.admit_date, 
			c.disch_date,
			c.dstatus as discharge_status, 
			c.diag1  as primary_diagnosis_cd, 
			c.proc1  as primary_icd_proc_cd,
			c.charge as total_charge_amount, 
			c.std_cost as total_allowed_amount,		
			duai.admission_id_src, 
			duai. member_id_src,
			'dim_uth_admission_id , optum_zip.confinement' as table_id_src		
		from data_warehouse.dim_uth_admission_id duai 
		inner join optum_zip.confinement c on c.conf_id = duai.admission_id_src 
											and cast(c.patid as text) = duai.member_id_src 
											and c."year" = duai."year" 	
		where data_source = 'optd'	
			and duai.uth_admission_id not in (select uth_admission_id 
												from dw_qa.admission_header 
												where data_source = 'optd')
--------------------------------------------------------------------------------------------		
 
 
	--check for duplicates											
	select uth_admission_id , count(uth_admission_id )
		from dw_qa.admission_header
		where data_source = 'optd'	
		group  by uth_admission_id , "year" 
		having count (uth_admission_id ) > 1
		
	select uth_admission_id , count(uth_admission_id )
		from dw_qa.admission_header
		where data_source = 'optz'	
		group  by uth_admission_id , "year" 
		having count (uth_admission_id ) > 1
														
														
	
		
		
		
	 