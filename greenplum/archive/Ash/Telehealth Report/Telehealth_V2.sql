
truncate table test.[UTHOUSTON\amoosa1].am_TeleHealthResult;
drop table if exists test.[UTHOUSTON\amoosa1].am_TeleHealthAllClaims ;

select distinct CLMID, DERV_COST , POS, PROCMOD , PROCMOD2 , PROCMOD3 , PROCMOD4, zm.PROV,
		zp.TAXONOMY1 , zp.TAXONOMY2 
	into test.[UTHOUSTON\amoosa1].am_TeleHealthAllClaims
	from OPT_ZIP_TX.dbo.Zip_Medical_2019 zm 
	inner join OPT_ZIP_TX.dbo.zip5_provider_bridge zpb on zm.PROV = zpb.PROV 
	inner join OPT_ZIP_TX.dbo.zip5_provider zp on zp.PROV_UNIQUE = zpb.PROV_UNIQUE 
	where POS in (2,11,17,20,49,50,53,71,72)
		and (CONF_ID is null or CONF_ID = '') ;
	
---------------------------------------------------------------------------------------------------------
--All Claims
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult			
	select 'All', count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge
		from test.[UTHOUSTON\amoosa1].am_TeleHealthAllClaims;
		
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult	
	select 'All TeleHealth', count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge 
		from test.[UTHOUSTON\amoosa1].am_TeleHealthAllClaims
		where POS = 2 or 
			(
				PROCMOD in ('95', 'GQ', 'GT') or 
				PROCMOD2 in ('95', 'GQ', 'GT') or 
				PROCMOD3 in ('95', 'GQ', 'GT') or 
				PROCMOD4 in ('95', 'GQ', 'GT')
		 	);
---------------------------------------------------------------------------------------------------------
--Physician
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult			
	select 'Physician', count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge
		from test.[UTHOUSTON\amoosa1].am_TeleHealthAllClaims
		where TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'Physician') or 
			  TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'Physician');
				
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult	
	select 'Physician TeleHealth', count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge 
		from test.[UTHOUSTON\amoosa1].am_TeleHealthAllClaims
		where (
				POS = 2 or 
				(
					PROCMOD in ('95', 'GQ', 'GT') or 
					PROCMOD2 in ('95', 'GQ', 'GT') or 
					PROCMOD3 in ('95', 'GQ', 'GT') or 
					PROCMOD4 in ('95', 'GQ', 'GT')
			 	)
			  ) 
			  and 
			  (
			  	TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'Physician') or 
			  	TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'Physician')
			  );
			 
---------------------------------------------------------------------------------------------------------			 
--PA or NP
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult			
	select 'PA or NP', count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge
		from test.[UTHOUSTON\amoosa1].am_TeleHealthAllClaims
		where TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'PA or NP') or 
			  TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'PA or NP');
				
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult	
	select 'PA or NP TeleHealth', count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge 
		from test.[UTHOUSTON\amoosa1].am_TeleHealthAllClaims
		where (
				POS = 2 or 
				(
					PROCMOD in ('95', 'GQ', 'GT') or 
					PROCMOD2 in ('95', 'GQ', 'GT') or 
					PROCMOD3 in ('95', 'GQ', 'GT') or 
					PROCMOD4 in ('95', 'GQ', 'GT')
			 	)
			  ) 
			  and 
			  (
			  	TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'PA or NP') or 
			  	TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'PA or NP')
			  );
		 	 
---------------------------------------------------------------------------------------------------------			 
--Psychiatrist ??
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult			
	select 'Psychiatrist ?', count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge
		from test.[UTHOUSTON\amoosa1].am_TeleHealthAllClaims
		where TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'psychiatrist') or 
			  TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'psychiatrist');
		
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult	
	select 'Psychiatrist TeleHealth ?', count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge 
		from test.[UTHOUSTON\amoosa1].am_TeleHealthAllClaims
		where (
				POS = 2 or 
				(
					PROCMOD in ('95', 'GQ', 'GT') or 
					PROCMOD2 in ('95', 'GQ', 'GT') or 
					PROCMOD3 in ('95', 'GQ', 'GT') or 
					PROCMOD4 in ('95', 'GQ', 'GT')
			 	)
			  ) 
			  and 
			  (
			  	TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'psychiatrist') or 
			  	TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'psychiatrist')
			  );
			 
---------------------------------------------------------------------------------------------------------			 
--Psychologist
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult			
	select 'Psychologist', count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge
		from test.[UTHOUSTON\amoosa1].am_TeleHealthAllClaims
		where TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'psychologist') or 
			  TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'psychologist');
				
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult	
	select 'Psychologist TeleHealth', count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge 
		from test.[UTHOUSTON\amoosa1].am_TeleHealthAllClaims
		where (
				POS = 2 or 
				(
					PROCMOD in ('95', 'GQ', 'GT') or 
					PROCMOD2 in ('95', 'GQ', 'GT') or 
					PROCMOD3 in ('95', 'GQ', 'GT') or 
					PROCMOD4 in ('95', 'GQ', 'GT')
			 	)
			  ) 
			  and 
			  (
			  	TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'psychologist') or 
			  	TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'psychologist')
			  );
			 
---------------------------------------------------------------------------------------------------------			 
--Occupational Therapist
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult			
	select 'Occupational Therapist', count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge
		from test.[UTHOUSTON\amoosa1].am_TeleHealthAllClaims
		where TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'occ therapist') or 
			  TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'occ therapist');
				
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult	
	select 'Occupational Therapist TeleHealth', count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge 
		from test.[UTHOUSTON\amoosa1].am_TeleHealthAllClaims
		where (
				POS = 2 or 
				(
					PROCMOD in ('95', 'GQ', 'GT') or 
					PROCMOD2 in ('95', 'GQ', 'GT') or 
					PROCMOD3 in ('95', 'GQ', 'GT') or 
					PROCMOD4 in ('95', 'GQ', 'GT')
			 	)
			  ) 
			  and 
			  (
			  	TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'occ therapist') or 
			  	TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'occ therapist')
			  );
			 
---------------------------------------------------------------------------------------------------------				 
--Physical Therapist
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult			
	select 'Physical Therapist', count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge
		from test.[UTHOUSTON\amoosa1].am_TeleHealthAllClaims
		where TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'PT') or 
			  TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'PT');
				
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult	
	select 'Physical Therapist TeleHealth', count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge 
		from test.[UTHOUSTON\amoosa1].am_TeleHealthAllClaims
		where (
				POS = 2 or 
				(
					PROCMOD in ('95', 'GQ', 'GT') or 
					PROCMOD2 in ('95', 'GQ', 'GT') or 
					PROCMOD3 in ('95', 'GQ', 'GT') or 
					PROCMOD4 in ('95', 'GQ', 'GT')
			 	)
			  ) 
			  and 
			  (
			  	TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'PT') or 
			  	TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'PT')
			  );
			 
---------------------------------------------------------------------------------------------------------		 
--Dietician Nutritionist
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult			
	select 'Dietician Nutritionist', count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge
		from test.[UTHOUSTON\amoosa1].am_TeleHealthAllClaims
		where TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'dietician nutritionist') or 
			  TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'dietician nutritionist');
				
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult	
	select 'Dietician Nutritionist TeleHealth', count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge 
		from test.[UTHOUSTON\amoosa1].am_TeleHealthAllClaims
		where (
				POS = 2 or 
				(
					PROCMOD in ('95', 'GQ', 'GT') or 
					PROCMOD2 in ('95', 'GQ', 'GT') or 
					PROCMOD3 in ('95', 'GQ', 'GT') or 
					PROCMOD4 in ('95', 'GQ', 'GT')
			 	)
			  ) 
			  and 
			  (
			  	TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'dietician nutritionist') or 
			  	TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'dietician nutritionist')
			  );
			 
---------------------------------------------------------------------------------------------------------		 
--Audiologist
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult			
	select 'Audiologist', count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge
		from test.[UTHOUSTON\amoosa1].am_TeleHealthAllClaims
		where TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'audiologist') or 
			  TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'audiologist');
				
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult	
	select 'Audiologist TeleHealth', count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge 
		from test.[UTHOUSTON\amoosa1].am_TeleHealthAllClaims
		where (
				POS = 2 or 
				(
					PROCMOD in ('95', 'GQ', 'GT') or 
					PROCMOD2 in ('95', 'GQ', 'GT') or 
					PROCMOD3 in ('95', 'GQ', 'GT') or 
					PROCMOD4 in ('95', 'GQ', 'GT')
			 	)
			  ) 
			  and 
			  (
			  	TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'audiologist') or 
			  	TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'audiologist')
			  );
			 
---------------------------------------------------------------------------------------------------------		 		 
		
select * from test.[UTHOUSTON\amoosa1].am_TeleHealthResult;	
		
 
----------------------------------------------------------------------------------------------------------
 





