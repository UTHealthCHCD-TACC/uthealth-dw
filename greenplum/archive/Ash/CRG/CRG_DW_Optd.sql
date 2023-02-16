drop table if exists dev.am_crg_dw_optd_members;
drop table if exists dev.am_crg_dw_optd_claim_detail;
drop table if exists dev.am_crg_dw_optd_claim_diag_distinct;
drop table if exists dev.am_crg_dw_optd_claim_primary_diag;
drop table if exists dev.am_crg_dw_optd_claim_secondary_diag;
drop table if exists dev.am_crg_dw_optd_claim_procs_distinct;
drop table if exists dev.am_crg_dw_optd_claim_external_cause_diagnosis;
drop table if exists dev.am_crg_dw_optd_claim_procs;
drop table if exists dev.am_crg_dw_optd_claim_cpt_hcpcs;
drop table if exists dev.am_crg_dw_optd_claims;
drop table if exists dev.am_crg_dw_optd_final;
------------------------------------------------------------------------------
--get members
drop table if exists dev.am_crg_dw_optd_members;
	select distinct data_source, 
			year as data_year, 
			uth_member_id, 			
			mem.gender_cd, 
			dob_derived
		into dev.am_crg_dw_optd_members
		from data_warehouse.member_enrollment_monthly mem 	
		where data_source = 'optd' 
			and year = 2019
			and age_derived between 0 and 15;
			--and age_derived between 16 and 30;	
			--and age_derived between 31 and 45;
			--and age_derived between 46 and 55;
			--and age_derived between 56 and 65;						
			--and age_derived >= 66;		

--select count(*) from dev.am_crg_dw_optd_members
------------------------------------------------------------------------------			
--get claim detail
drop table if exists dev.am_crg_dw_optd_claim_detail;
	select distinct cd.uth_claim_id ,
			cd.claim_id_src ,
			cd.uth_member_id ,
			cd.member_id_src ,			
			coalesce(cd.claim_sequence_number , cast(cd.claim_sequence_number_src as int4) , row_number() over (order by cd.claim_id_src )) as claim_sequence_number,
			--cd.claim_sequence_number,
			cd.admit_date ,
			cd.discharge_date ,
			cd.from_date_of_service ,
			cd.to_date_of_service ,
			cd.cpt_hcpcs ,
			case when cd.bill_type_inst is null then '2' else cd.bill_type_inst  end as provider_type	,
			cd.place_of_service ,
			cd.discharge_status ,
			'0' || 
				case when cd.bill_type_inst is null then '0' else cd.bill_type_inst end || 
				case when cd.bill_type_class is null then '0' else cd.bill_type_class  end || 				
				case when cd.bill_type_inst is null and cd.bill_type_class is null then '0'
					 when cd.bill_type_inst is not null and cd.bill_type_inst <> '0' and  
					      cd.bill_type_class is not null and cd.bill_type_class <> '0' then 
																						case when cd.bill_type_freq is null and (cd.data_source = 'optz' or cd.data_source = 'optd') then '1' 
																							else coalesce(cd.bill_type_freq, '0')
																						end										 
				else coalesce(cd.bill_type_freq, '0') end				
			as type_of_bill, --missing data in bill_type_freq
			cd.revenue_cd 
		into dev.am_crg_dw_optd_claim_detail
		from data_warehouse.claim_detail cd , dev.am_crg_dw_optd_members m 
		where cd.uth_member_id = m.uth_member_id 
			and cd.data_source = 'optd'			
			and cd.year = 2019
		order by cd.uth_claim_id, cd.from_date_of_service;

--select * from dev.am_crg_dw_optd_claim_detail d order by uth_claim_id, claim_sequence_number
------------------------------------------------------------------------------
--get distinct claim diagnosis in temp table
drop table if exists dev.am_crg_dw_optd_claim_diag_distinct;	

select distinct cd.data_source,cd.year, cd.uth_member_id, cd.uth_claim_id, cd.claim_sequence_number, cd.from_date_of_service , diag_cd, diag_position, icd_type, poa_src 
	into dev.am_crg_dw_optd_claim_diag_distinct
	from data_warehouse.claim_diag cd , dev.am_crg_dw_optd_claim_detail d
	where cd.uth_claim_id = d.uth_claim_id
		and cd.uth_member_id = d.uth_member_id
		and cd.data_source = 'optd'			
		and cd.year = 2019;
	
--select * from dev.am_crg_dw_optd_claim_diag_distinct	
------------------------------------------------------------------------------	
drop table if exists dev.am_crg_dw_optd_claim_primary_diag;
	
	select distinct uth_claim_id , diag_cd , icd_type 		
		into dev.am_crg_dw_optd_claim_primary_diag
		from dev.am_crg_dw_optd_claim_diag_distinct 
		where diag_position = 1
			and claim_sequence_number = 1;		
 
--select * from dev.am_crg_dw_optd_claim_primary_diag
------------------------------------------------------------------------------
--concate secondary claim diagnosis	
drop table if exists dev.am_crg_dw_optd_claim_secondary_diag;
with cte as (
				select d.uth_claim_id , 					
						string_agg(d.diag_cd , ';' order by d.claim_sequence_number , d.diag_position ) diags 
					from dev.am_crg_dw_optd_claim_diag_distinct d
					where d.diag_position != 1						
					group by d.uth_claim_id
				)				
	select distinct uth_claim_id , diags		
		into dev.am_crg_dw_optd_claim_secondary_diag
		from cte;

--select * from dev.am_crg_dw_optd_claim_secondary_diag			
------------------------------------------------------------------------------
--get ExternalCauseOfInjuryDiagnosis
drop table if exists dev.am_crg_dw_optd_claim_external_cause_diagnosis;	
	
with ctex as (
			select d.uth_claim_id , 					
					string_agg(d.diag_cd , ';' order by d.claim_sequence_number , d.diag_position ) diags 
				from dev.am_crg_dw_optd_claim_diag_distinct d
				where (d.diag_cd like 'E%' and d.icd_type = '9')
					or (
							(d.diag_cd like 'V%' or d.diag_cd like 'X%' or d.diag_cd like 'Y%' or d.diag_cd like 'W%')							
							and 
							(d.icd_type = '0' or d.icd_type = '10' or d.icd_type is null)							
						)
				group by d.uth_claim_id
			)		
 
select distinct uth_claim_id , diags		
		into dev.am_crg_dw_optd_claim_external_cause_diagnosis
		from ctex;
 	
------------------------------------------------------------------------------
--get distinct claim procs
drop table if exists dev.am_crg_dw_optd_claim_procs_distinct;
		
select distinct data_source, "year", p.uth_claim_id, p.uth_member_id, p.claim_sequence_number, p.from_date_of_service , proc_cd, proc_position, icd_type
	into dev.am_crg_dw_optd_claim_procs_distinct
	from data_warehouse.claim_icd_proc p , dev.am_crg_dw_optd_claim_detail cd
	where p.uth_claim_id = cd.uth_claim_id 
		and p.uth_member_id = cd.uth_member_id 
		and p.data_source = 'optd'			
		and p.year = 2019;

--select * from dev.am_crg_dw_optd_claim_procs_distinct;	
------------------------------------------------------------------------------			
--concate claim procedures
drop table if exists dev.am_crg_dw_optd_claim_procs;					
with cte2 as (
				select p.uth_claim_id , 						
						string_agg(p.proc_cd , ';' order by p.claim_sequence_number, p.proc_position ) procs,
						max(p.from_date_of_service) as procedure_date
					from dev.am_crg_dw_optd_claim_procs_distinct p					
					group by p.uth_claim_id 
				)				
	select distinct uth_claim_id, 			
			procs,
			procedure_date
		into dev.am_crg_dw_optd_claim_procs
		from cte2;

--select * from dev.am_crg_dw_optd_claim_procs	
------------------------------------------------------------------------------	
--concate cpt_hcpcs
drop table if exists dev.am_crg_dw_optd_claim_cpt_hcpcs;					
with cte3 as (
				select cd.uth_claim_id , 						
						string_agg(cd.cpt_hcpcs , ';' order by cd.claim_sequence_number ) cpt_hcpcs
					from dev.am_crg_dw_optd_claim_detail cd	
					where cd.cpt_hcpcs is not null
					group by cd.uth_claim_id 
				)				
	select distinct uth_claim_id, 			
			cpt_hcpcs			
		into dev.am_crg_dw_optd_claim_cpt_hcpcs
		from cte3 	;

--select * from dev.am_crg_dw_optd_claim_cpt_hcpcs
------------------------------------------------------------------------------		
--get distinct claimids
drop table if exists dev.am_crg_dw_optd_claims;
	select distinct d.uth_claim_id , 		
			max(claim_sequence_number ) as idx
		into dev.am_crg_dw_optd_claims
		from dev.am_crg_dw_optd_claim_detail d
		group by d.uth_claim_id ;

--select * from dev.am_crg_dw_optd_claims	
------------------------------------------------------------------------------		
--create final result table	
drop table if exists dev.am_crg_dw_optd_final;
	select distinct 
			c.member_id_src as PatientId, 																--1			
			dev.fn_get_crg_valid_cd('optd', 'sex', m.gender_cd) as Sex, 								--2
			to_char(m.dob_derived,'MMDDYYYY') as BirthDate, 											--3		 
			c.claim_id_src as ClaimId, 																	--4
			to_char( case when c.admit_date is null then c.from_date_of_service else c.admit_date end ,'MMDDYYYY') as AdmitDate,	--5						
			to_char( case when c.discharge_date is null then c.to_date_of_service else c.discharge_date end ,'MMDDYYYY') as Dischargedate,	 --6 			
			to_char(c.from_date_of_service ,'MMDDYYYY') as ItemFromDate,								--7	 
			to_char(c.to_date_of_service ,'MMDDYYYY') as ItemToDate,									--8		 
			case when c.discharge_date is not null then dev.fn_get_crg_valid_cd('optd', 'dischargestatus', c.discharge_status)
				else null 
			end as DischargeStatus,																		--9	
			c.type_of_bill as TypeOfBill,																--10
			dev.fn_get_crg_valid_cd('optd', 'placeofservice', c.place_of_service) as PlaceOfService,	--11
			c.provider_type as ProviderType,															--12
			dev.fn_get_crg_valid_cd('optd', 'icdversionqualifier', pd.icd_type) as ICDVersionQualifier,	--13
			case when c.admit_date is not null and pd.diag_cd is not null then pd.diag_cd else null end as AdmitDiagnosis,	--14
			pd.diag_cd as PrincipalDiagnosis,															--15
			sd.diags as SecondaryDiagnosis,																--16
			ed.diags as ExternalCauseOfInjuryDiagnosis,														--17
	 		'' as ReasonForVisitDiagnosis,																--18
	 		p.procs as "Procedure",																		--19
	 		to_char(p.procedure_date ,'MMDDYYYY') as "Procedure	Date",									--20
	 	 	h.cpt_hcpcs as ProcedureHcpcs,																--21
	 	 	'' as ItemDiagnosisPointer,																	--22
	 	 	dev.fn_get_crg_valid_cd('optd', 'placeofservice', c.place_of_service) as ItemPlaceOfService,--23 
	 	 	c.provider_type as ItemProviderType, 														--24
	 	 	to_char(c.to_date_of_service ,'MMDDYYYY') as ItemServiceDate,								--25
			'' as ItemNdcCode,																			--26
			'' as ItemAtcCode,																			--27
			'' as ItemDinCode,																			--28
			'' as FunctionalStatusGrouperDisable,														--29
			'' as FunctionalStatusGrouperAssessmentDate,												--30
			'' as FunctionalStatusGrouperAssessmentTool,												--31
			'' as FunctionalStatusGrouperAssessmentItemId,												--32
			'' as FunctionalStatusGrouperAssessmentScore,												--33
			c.revenue_cd as ItemRevenueCode,															--34
			dev.fn_get_crg_valid_cd('optd', 'itemsiteofservice', c.place_of_service) as ItemSiteOfService,	--35
			dev.fn_get_crg_valid_cd('optd', 'siteofservice', c.place_of_service) as SiteOfService		--36
			--'\n' as "NewLine"																			--37
		into dev.am_crg_dw_optd_final																	
		from dev.am_crg_dw_optd_members m 	 
		inner join dev.am_crg_dw_optd_claim_detail c on m.uth_member_id = c.uth_member_id	
		inner join dev.am_crg_dw_optd_claims cl on cl.uth_claim_id = c.uth_claim_id 
		inner join dev.am_crg_dw_optd_claim_primary_diag pd on pd.uth_claim_id = c.uth_claim_id 
		left join dev.am_crg_dw_optd_claim_secondary_diag sd on sd.uth_claim_id = c.uth_claim_id 
		left join dev.am_crg_dw_optd_claim_procs p on p.uth_claim_id = c.uth_claim_id 
		left join dev.am_crg_dw_optd_claim_cpt_hcpcs h on h.uth_claim_id = c.uth_claim_id 
		left join dev.am_crg_dw_optd_claim_external_cause_diagnosis ed on ed.uth_claim_id = c.uth_claim_id 
		where c.uth_claim_id is not null	
			and cl.idx = c.claim_sequence_number		
		order by 1, 7 ;
	
---------------------------------------------------------------------------------------------------------------------	
--select distinct * from dev.am_crg_dw_optd_final order by 1, 7 ;
  
