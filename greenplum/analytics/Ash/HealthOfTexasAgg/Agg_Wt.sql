declare @Year int = 2017

--for first year to create the table
/*
drop table if exists [UTHOUSTON\amoosa1].[am_HTexas_Zip3_Agg]

select 'TrvTX          ' as DataSource, @Year as DataYear, count(distinct EnrolID) as TotalCount, convert(varchar(5),EMPZIP) as Zip3
	into  [UTHOUSTON\amoosa1].[am_HTexas_Zip3_Agg]
	from CND.[dbo].[AGG_ENRL_TRVTX] a
	inner join [test].[UTHOUSTON\amoosa1].[am_HTexas_zip3] z on z.Zip = a.EMPZIP
	where YEAR = @Year
	group by EMPZIP

*/

insert into  [UTHOUSTON\amoosa1].[am_HTexas_Zip3_Agg]
select 'TrvTX' as DataSource, @Year as DataYear, count(distinct EnrolID) as TotalCount, convert(varchar(5),EMPZIP) as Zip3
	from CND.[dbo].[AGG_ENRL_TRVMS] a
	inner join [test].[UTHOUSTON\amoosa1].[am_HTexas_zip3] z on z.Zip = a.EMPZIP
	where YEAR = @Year
	group by EMPZIP

	   	 






insert into  [UTHOUSTON\amoosa1].[am_HTexas_Zip3_Agg]
select 'TrvMS' as DataSource, @Year as DataYear, count(distinct EnrolID) as TotalCount, EMPZIP as Zip3
	from CND.[dbo].[AGG_ENRL_TRVMS] a
	inner join [test].[UTHOUSTON\amoosa1].[am_HTexas_zip3] z on z.Zip = a.EMPZIP
	where YEAR = @Year
	group by EMPZIP
	
--to handle case sensitive ids
;with cte as
	(
		select BENE_ID COLLATE Latin1_General_CS_AS as BENE_ID, SUBSTRING(ZIP_CD, 1, 3) as Zip3
			from CND.[dbo].[AGG_ENRL_Medicare] a
			inner join [test].[UTHOUSTON\amoosa1].[am_HTexas_zip3] z on z.Zip = SUBSTRING(ZIP_CD, 1, 3)
			where ENRL_YEAR = @Year
	)	
insert into  [UTHOUSTON\amoosa1].[am_HTexas_Zip3_Agg]
select 'Medicare' as DataSource, @Year as DataYear, count(BENE_ID) as TotalCount,  Zip3 
	from cte
	group by Zip3

	
insert into  [UTHOUSTON\amoosa1].[am_HTexas_Zip3_Agg]
select 'Medicaid' as DataSource, @Year as DataYear, count(distinct CLIENT_NBR)  as TotalCount, ZIP3 
	from cnd.[dbo].[AGG_ENRL_Medicaid_CY1219] a
	inner join [test].[UTHOUSTON\amoosa1].[am_HTexas_zip3] z on z.Zip = a.ZIP3
	where ENRL_CY = @Year
	group by ZIP3



insert into  [UTHOUSTON\amoosa1].[am_HTexas_Zip3_Agg]
select 'OPTZIP_COM' as DataSource, @Year as DataYear, count(distinct PATID) as TotalCount,  SUBSTRING(ZIPCODE_5, 1, 3)  as Zip3
	from OPT_ZIP_TX.dbo.AGG_ENRL_OPTZIPTX
	inner join [test].[UTHOUSTON\amoosa1].[am_HTexas_zip3] z on z.Zip = SUBSTRING(ZIPCODE_5, 1, 3)  
	where ENRL_YEAR = @Year
		and BUS = 'COM'
		and AGE < 65
	group by  SUBSTRING(ZIPCODE_5, 1, 3)  


insert into  [UTHOUSTON\amoosa1].[am_HTexas_Zip3_Agg]
select 'OPTZIP_MCR' as DataSource, @Year as DataYear, count(distinct PATID) as TotalCount, SUBSTRING(ZIPCODE_5, 1, 3)  as Zip3
	from OPT_ZIP_TX.dbo.AGG_ENRL_OPTZIPTX
	inner join [test].[UTHOUSTON\amoosa1].[am_HTexas_zip3] z on z.Zip = SUBSTRING(ZIPCODE_5, 1, 3)  
	where ENRL_YEAR = @Year
		and BUS = 'MCR'
		and AGE >= 65
	group by  SUBSTRING(ZIPCODE_5, 1, 3)  

------------------------------------------------------------------------------------------------------------
insert into  [UTHOUSTON\amoosa1].[am_HTexas_Zip3_Agg]
	select 'Medicaid' as DataSource, @Year as DataYear, count(distinct CLIENT_NBR) as TotalCount, 'All' as Zip3
		from cnd.[dbo].[AGG_ENRL_Medicaid_CY1219]
		where ENRL_CY=@Year 
			and Zip3 in (select zip from [test].[UTHOUSTON\amoosa1].[am_HTexas_zip3] )


insert into  [UTHOUSTON\amoosa1].[am_HTexas_Zip3_Agg]
	select 'OPTZIP_COM' as DataSource, @Year as DataYear, count(distinct PATID) as TotalCount, 'All' as Zip3
		from OPT_ZIP_TX.dbo.AGG_ENRL_OPTZIPTX
		inner join [test].[UTHOUSTON\amoosa1].[am_HTexas_zip3] z on z.Zip = SUBSTRING(ZIPCODE_5, 1, 3)  
		where ENRL_YEAR = @Year
			and BUS = 'COM'
			and AGE < 65


insert into  [UTHOUSTON\amoosa1].[am_HTexas_Zip3_Agg]
	select 'OPTZIP_MCR' as DataSource, @Year as DataYear, count(distinct PATID) as TotalCount, 'All' as Zip3
		from OPT_ZIP_TX.dbo.AGG_ENRL_OPTZIPTX
		inner join [test].[UTHOUSTON\amoosa1].[am_HTexas_zip3] z on z.Zip = SUBSTRING(ZIPCODE_5, 1, 3)  
		where ENRL_YEAR = @Year
			and BUS = 'MCR'
			and AGE >= 65

insert into  [UTHOUSTON\amoosa1].[am_HTexas_Zip3_Agg]
	select 'TrvTX' as DataSource, @Year as DataYear, count(distinct EnrolID) as TotalCount, 'All' as Zip3 
		from CND.[dbo].[AGG_ENRL_TRVTX] a
		inner join [test].[UTHOUSTON\amoosa1].[am_HTexas_zip3] z on z.Zip = a.EMPZIP
		where YEAR = @Year

insert into  [UTHOUSTON\amoosa1].[am_HTexas_Zip3_Agg]
	select 'TrvMS' as DataSource, @Year as DataYear, count(distinct EnrolID) as TotalCount, 'All' as Zip3
		from CND.[dbo].[AGG_ENRL_TRVMS] a
		inner join [test].[UTHOUSTON\amoosa1].[am_HTexas_zip3] z on z.Zip = a.EMPZIP
		where YEAR = @Year
		 
--to handle case sensitive ids
;with cte as
	(
		select BENE_ID COLLATE Latin1_General_CS_AS as BENE_ID, SUBSTRING(ZIP_CD, 1, 3) as Zip3
			from CND.[dbo].[AGG_ENRL_Medicare] a
			inner join [test].[UTHOUSTON\amoosa1].[am_HTexas_zip3] z on z.Zip = SUBSTRING(ZIP_CD, 1, 3)
			where ENRL_YEAR = @Year
	)	
insert into  [UTHOUSTON\amoosa1].[am_HTexas_Zip3_Agg]
select 'Medicare' as DataSource, @Year as DataYear, count(distinct BENE_ID) as TotalCount, 'All' as Zip3
	from cte
 	 
------------------------------------------------------------------------------------------------------------

select * from [UTHOUSTON\amoosa1].[am_HTexas_Zip3_Agg] order by Zip3, DataYear
------------------------------------------------------------------------------------------------------------------
 