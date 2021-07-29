
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
			 
			 
			 	 
		 
		 
		 
		 
		 
		 
		
select * from test.[UTHOUSTON\amoosa1].am_TeleHealthResult;	
		
		
			
with cte as (
	select count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge, pos
		from OPT_ZIP_TX.dbo.Zip_Medical_2019 zm 
		where POS in (2,11,17,20,49,50,53,71,72)
			and (CONF_ID is null or CONF_ID = '')
		group by pos 
)
--select * from cte
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult
	select 'All', sum (TotalClaims) as TotalClaims, sum(TotalCharge) as TotalCharge from cte;
 

with cte as (
	select count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge, pos
		from OPT_ZIP_TX.dbo.Zip_Medical_2019 zm 
		where (CONF_ID is null or CONF_ID = '')
			and (
					POS = 2 or 
					(
						POS in (11,17,20,49,50,53,71,72) and 
						(
		 					 zm.PROCMOD in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD2 in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD3 in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD4 in ('95', 'GQ', 'GT')
						 )	
		 			)
		 		)
		group by pos 
)
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult
	select 'All TeleHealth' , sum (TotalClaims) as TeleHealthTotalClaims, sum(TotalCharge) as TeleHealthTotalCharge  from cte;	
 

----------------------------------------------------------------------------------------------------------
--Physician
with cte as (
	select count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge, pos
		from OPT_ZIP_TX.dbo.Zip_Medical_2019 zm
		inner join OPT_ZIP_TX.dbo.zip5_provider_bridge zpb on zm.PROV = zpb.PROV 
		inner join OPT_ZIP_TX.dbo.zip5_provider zp on zp.PROV_UNIQUE = zpb.PROV_UNIQUE 
		where POS in (2,11,17,20,49,50,53,71,72)
			and (CONF_ID is null or CONF_ID = '')
			and (
					zp.TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'Physician') or 
					zp.TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'Physician')
				)
		group by pos 
)
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult
	select 'Physician', sum (TotalClaims) as TotalClaims, sum(TotalCharge) as TotalCharge from cte;
	
	
with cte as (
	select count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge, pos
		from OPT_ZIP_TX.dbo.Zip_Medical_2019 zm 
		inner join OPT_ZIP_TX.dbo.zip5_provider_bridge zpb on zm.PROV = zpb.PROV 
		inner join OPT_ZIP_TX.dbo.zip5_provider zp on zp.PROV_UNIQUE = zpb.PROV_UNIQUE 
		where (CONF_ID is null or CONF_ID = '')
			and (
					POS = 2 or 
					(
						POS in (11,17,20,49,50,53,71,72) and 
						(
		 					 zm.PROCMOD in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD2 in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD3 in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD4 in ('95', 'GQ', 'GT')
						 )	
		 			)
		 		)
		 	and (
					zp.TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'Physician') or 
					zp.TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'Physician')
				)
		group by pos 
)
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult
	select 'Physician TeleHealth' , sum (TotalClaims) as TeleHealthTotalClaims, sum(TotalCharge) as TeleHealthTotalCharge  from cte;	
 
----------------------------------------------------------------------------------------------------------
--PA or NP
with cte as (
	select count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge, pos
		from OPT_ZIP_TX.dbo.Zip_Medical_2019 zm
		inner join OPT_ZIP_TX.dbo.zip5_provider_bridge zpb on zm.PROV = zpb.PROV 
		inner join OPT_ZIP_TX.dbo.zip5_provider zp on zp.PROV_UNIQUE = zpb.PROV_UNIQUE 
		where POS in (2,11,17,20,49,50,53,71,72)
			and (CONF_ID is null or CONF_ID = '')
			and (
					zp.TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'PA or NP') or 
					zp.TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'PA or NP')
				)
		group by pos 
)
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult
	select 'PA or NP', sum (TotalClaims) as TotalClaims, sum(TotalCharge) as TotalCharge from cte;
	
	
with cte as (
	select count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge, pos
		from OPT_ZIP_TX.dbo.Zip_Medical_2019 zm 
		inner join OPT_ZIP_TX.dbo.zip5_provider_bridge zpb on zm.PROV = zpb.PROV 
		inner join OPT_ZIP_TX.dbo.zip5_provider zp on zp.PROV_UNIQUE = zpb.PROV_UNIQUE 
		where (CONF_ID is null or CONF_ID = '')
			and (
					POS = 2 or 
					(
						POS in (11,17,20,49,50,53,71,72) and 
						(
		 					 zm.PROCMOD in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD2 in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD3 in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD4 in ('95', 'GQ', 'GT')
						 )	
		 			)
		 		)
		 	and (
					zp.TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'PA or NP') or 
					zp.TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'PA or NP')
				)
		group by pos 
)
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult
	select 'PA or NP TeleHealth' , sum (TotalClaims) as TeleHealthTotalClaims, sum(TotalCharge) as TeleHealthTotalCharge  from cte;

----------------------------------------------------------------------------------------------------------
--other professional
with cte as (
	select count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge, pos
		from OPT_ZIP_TX.dbo.Zip_Medical_2019 zm
		inner join OPT_ZIP_TX.dbo.zip5_provider_bridge zpb on zm.PROV = zpb.PROV 
		inner join OPT_ZIP_TX.dbo.zip5_provider zp on zp.PROV_UNIQUE = zpb.PROV_UNIQUE 
		where POS in (2,11,17,20,49,50,53,71,72)
			and (CONF_ID is null or CONF_ID = '')
			and (
					zp.TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'other professional') or 
					zp.TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'other professional')
				)
		group by pos 
)
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult
	select 'Other Professional', sum (TotalClaims) as TotalClaims, sum(TotalCharge) as TotalCharge from cte;
	
	
with cte as (
	select count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge, pos
		from OPT_ZIP_TX.dbo.Zip_Medical_2019 zm 
		inner join OPT_ZIP_TX.dbo.zip5_provider_bridge zpb on zm.PROV = zpb.PROV 
		inner join OPT_ZIP_TX.dbo.zip5_provider zp on zp.PROV_UNIQUE = zpb.PROV_UNIQUE 
		where (CONF_ID is null or CONF_ID = '')
			and (
					POS = 2 or 
					(
						POS in (11,17,20,49,50,53,71,72) and 
						(
		 					 zm.PROCMOD in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD2 in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD3 in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD4 in ('95', 'GQ', 'GT')
						 )	
		 			)
		 		)
		 	and (
					zp.TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'other professional') or 
					zp.TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'other professional')
				)
		group by pos 
)
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult
	select 'Other Professional TeleHealth' , sum (TotalClaims) as TeleHealthTotalClaims, sum(TotalCharge) as TeleHealthTotalCharge  from cte;

----------------------------------------------------------------------------------------------------------
--psychologist
with cte as (
	select count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge, pos
		from OPT_ZIP_TX.dbo.Zip_Medical_2019 zm
		inner join OPT_ZIP_TX.dbo.zip5_provider_bridge zpb on zm.PROV = zpb.PROV 
		inner join OPT_ZIP_TX.dbo.zip5_provider zp on zp.PROV_UNIQUE = zpb.PROV_UNIQUE 
		where POS in (2,11,17,20,49,50,53,71,72)
			and (CONF_ID is null or CONF_ID = '')
			and (
					zp.TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'psychologist') or 
					zp.TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'psychologist')
				)
		group by pos 
)
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult
	select 'Psychologist', sum (TotalClaims) as TotalClaims, sum(TotalCharge) as TotalCharge from cte;
	
	
with cte as (
	select count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge, pos
		from OPT_ZIP_TX.dbo.Zip_Medical_2019 zm 
		inner join OPT_ZIP_TX.dbo.zip5_provider_bridge zpb on zm.PROV = zpb.PROV 
		inner join OPT_ZIP_TX.dbo.zip5_provider zp on zp.PROV_UNIQUE = zpb.PROV_UNIQUE 
		where (CONF_ID is null or CONF_ID = '')
			and (
					POS = 2 or 
					(
						POS in (11,17,20,49,50,53,71,72) and 
						(
		 					 zm.PROCMOD in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD2 in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD3 in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD4 in ('95', 'GQ', 'GT')
						 )	
		 			)
		 		)
		 	and (
					zp.TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'psychologist') or 
					zp.TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'psychologist')
				)
		group by pos 
)
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult
	select 'Psychologist TeleHealth' , sum (TotalClaims) as TeleHealthTotalClaims, sum(TotalCharge) as TeleHealthTotalCharge  from cte;

----------------------------------------------------------------------------------------------------------
--Occupational Therapist
with cte as (
	select count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge, pos
		from OPT_ZIP_TX.dbo.Zip_Medical_2019 zm
		inner join OPT_ZIP_TX.dbo.zip5_provider_bridge zpb on zm.PROV = zpb.PROV 
		inner join OPT_ZIP_TX.dbo.zip5_provider zp on zp.PROV_UNIQUE = zpb.PROV_UNIQUE 
		where POS in (2,11,17,20,49,50,53,71,72)
			and (CONF_ID is null or CONF_ID = '')
			and (
					zp.TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'occ therapist') or 
					zp.TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'occ therapist')
				)
		group by pos 
)
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult
	select 'Occupational Therapist', sum (TotalClaims) as TotalClaims, sum(TotalCharge) as TotalCharge from cte;
	
	
with cte as (
	select count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge, pos
		from OPT_ZIP_TX.dbo.Zip_Medical_2019 zm 
		inner join OPT_ZIP_TX.dbo.zip5_provider_bridge zpb on zm.PROV = zpb.PROV 
		inner join OPT_ZIP_TX.dbo.zip5_provider zp on zp.PROV_UNIQUE = zpb.PROV_UNIQUE 
		where (CONF_ID is null or CONF_ID = '')
			and (
					POS = 2 or 
					(
						POS in (11,17,20,49,50,53,71,72) and 
						(
		 					 zm.PROCMOD in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD2 in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD3 in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD4 in ('95', 'GQ', 'GT')
						 )	
		 			)
		 		)
		 	and (
					zp.TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'occ therapist') or 
					zp.TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'occ therapist')
				)
		group by pos 
)
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult
	select 'Occupational Therapist TeleHealth' , sum (TotalClaims) as TeleHealthTotalClaims, sum(TotalCharge) as TeleHealthTotalCharge  from cte;

----------------------------------------------------------------------------------------------------------
--Physical Therapist
with cte as (
	select count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge, pos
		from OPT_ZIP_TX.dbo.Zip_Medical_2019 zm
		inner join OPT_ZIP_TX.dbo.zip5_provider_bridge zpb on zm.PROV = zpb.PROV 
		inner join OPT_ZIP_TX.dbo.zip5_provider zp on zp.PROV_UNIQUE = zpb.PROV_UNIQUE 
		where POS in (2,11,17,20,49,50,53,71,72)
			and (CONF_ID is null or CONF_ID = '')
			and (
					zp.TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'PT') or 
					zp.TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'PT')
				)
		group by pos 
)
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult
	select 'Physical Therapist', sum (TotalClaims) as TotalClaims, sum(TotalCharge) as TotalCharge from cte;
	
	
with cte as (
	select count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge, pos
		from OPT_ZIP_TX.dbo.Zip_Medical_2019 zm 
		inner join OPT_ZIP_TX.dbo.zip5_provider_bridge zpb on zm.PROV = zpb.PROV 
		inner join OPT_ZIP_TX.dbo.zip5_provider zp on zp.PROV_UNIQUE = zpb.PROV_UNIQUE 
		where (CONF_ID is null or CONF_ID = '')
			and (
					POS = 2 or 
					(
						POS in (11,17,20,49,50,53,71,72) and 
						(
		 					 zm.PROCMOD in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD2 in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD3 in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD4 in ('95', 'GQ', 'GT')
						 )	
		 			)
		 		)
		 	and (
					zp.TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'PT') or 
					zp.TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'PT')
				)
		group by pos 
)
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult
	select 'Physical Therapist TeleHealth' , sum (TotalClaims) as TeleHealthTotalClaims, sum(TotalCharge) as TeleHealthTotalCharge  from cte;

----------------------------------------------------------------------------------------------------------
--Dietician Nutritionist
with cte as (
	select count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge, pos
		from OPT_ZIP_TX.dbo.Zip_Medical_2019 zm
		inner join OPT_ZIP_TX.dbo.zip5_provider_bridge zpb on zm.PROV = zpb.PROV 
		inner join OPT_ZIP_TX.dbo.zip5_provider zp on zp.PROV_UNIQUE = zpb.PROV_UNIQUE 
		where POS in (2,11,17,20,49,50,53,71,72)
			and (CONF_ID is null or CONF_ID = '')
			and (
					zp.TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'dietician nutritionist') or 
					zp.TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'dietician nutritionist')
				)
		group by pos 
)
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult
	select 'Dietician Nutritionist', sum (TotalClaims) as TotalClaims, sum(TotalCharge) as TotalCharge from cte;
	
	
with cte as (
	select count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge, pos
		from OPT_ZIP_TX.dbo.Zip_Medical_2019 zm 
		inner join OPT_ZIP_TX.dbo.zip5_provider_bridge zpb on zm.PROV = zpb.PROV 
		inner join OPT_ZIP_TX.dbo.zip5_provider zp on zp.PROV_UNIQUE = zpb.PROV_UNIQUE 
		where (CONF_ID is null or CONF_ID = '')
			and (
					POS = 2 or 
					(
						POS in (11,17,20,49,50,53,71,72) and 
						(
		 					 zm.PROCMOD in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD2 in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD3 in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD4 in ('95', 'GQ', 'GT')
						 )	
		 			)
		 		)
		 	and (
					zp.TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'dietician nutritionist') or 
					zp.TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'dietician nutritionist')
				)
		group by pos 
)
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult
	select 'Dietician Nutritionist TeleHealth' , sum (TotalClaims) as TeleHealthTotalClaims, sum(TotalCharge) as TeleHealthTotalCharge  from cte;

----------------------------------------------------------------------------------------------------------
--Audiologist
with cte as (
	select count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge, pos
		from OPT_ZIP_TX.dbo.Zip_Medical_2019 zm
		inner join OPT_ZIP_TX.dbo.zip5_provider_bridge zpb on zm.PROV = zpb.PROV 
		inner join OPT_ZIP_TX.dbo.zip5_provider zp on zp.PROV_UNIQUE = zpb.PROV_UNIQUE 
		where POS in (2,11,17,20,49,50,53,71,72)
			and (CONF_ID is null or CONF_ID = '')
			and (
					zp.TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'audiologist') or 
					zp.TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'audiologist')
				)
		group by pos 
)
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult
	select 'Audiologist', sum (TotalClaims) as TotalClaims, sum(TotalCharge) as TotalCharge from cte;
	
	
with cte as (
	select count(distinct CLMID) as TotalClaims, sum(DERV_COST) as TotalCharge, pos
		from OPT_ZIP_TX.dbo.Zip_Medical_2019 zm 
		inner join OPT_ZIP_TX.dbo.zip5_provider_bridge zpb on zm.PROV = zpb.PROV 
		inner join OPT_ZIP_TX.dbo.zip5_provider zp on zp.PROV_UNIQUE = zpb.PROV_UNIQUE 
		where (CONF_ID is null or CONF_ID = '')
			and (
					POS = 2 or 
					(
						POS in (11,17,20,49,50,53,71,72) and 
						(
		 					 zm.PROCMOD in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD2 in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD3 in ('95', 'GQ', 'GT') or 
							 zm.PROCMOD4 in ('95', 'GQ', 'GT')
						 )	
		 			)
		 		)
		 	and (
					zp.TAXONOMY1 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'audiologist') or 
					zp.TAXONOMY2 in (select Taxonomy from test.[UTHOUSTON\amoosa1].am_OPT_Taxonomy where Description = 'audiologist')
				)
		group by pos 
)
insert into test.[UTHOUSTON\amoosa1].am_TeleHealthResult
	select 'Audiologist TeleHealth' , sum (TotalClaims) as TeleHealthTotalClaims, sum(TotalCharge) as TeleHealthTotalCharge  from cte;

----------------------------------------------------------------------------------------------------------

select   * from test.[UTHOUSTON\amoosa1].am_TeleHealthResult;
	
	
	