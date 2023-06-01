/* ******************************************************************************************************
 *  The member_enrollment_monthly table creates one record for each month/year that a member was enrolled in coverage
 * 
 *  !!!!!!!!!  data_warehouse.dim_member_id_src table must be populated first !!!!!!!!!    
 *   	             See folder 01-setup
 * 	
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 1/01/2021 || script created 
 * ******************************************************************************************************
 *  wc002  || 6/28/21 || added logic to exclude enrollment records after death optum dod
 * ******************************************************************************************************
 *  wallingTACC  || 8/23/2021 || Cleaning up comments
 * ******************************************************************************************************
 *  wc003  || 9/02/2021 || Changing process to load dw_staging. Add mapping for null race to assign 0 (Unknown).
 * ******************************************************************************************************
 *  jw001  || 9/20/2021 || Cut to its own script file from longer file
 *  ******************************************************************************************************
 *  wc004  || 11/06/2021 || moved table creation to new script. formatting. changed bus_cd mapping
 *  ******************************************************************************************************
 *  xzhang || 05/25/23   || Tweaked for 2023
 * 							split medicare texas and medicare national into their own sql files
 * 							split into individual tables for easier insertion into DW
 * 							age_derived -> age_cy, and changed calculation method
 * 							added age_months, age_fy, table_id_src, member_id_src, load_date
 * 							added consecutive enrolled months
 * 							added a vacuum analyze
 *  ******************************************************************************************************
 *  xzhang || 05/31/23   || There was an issue with a join and I didn't want to try to troubleshoot the legacy code
 * 							so I re-wrote it so that an ETL is generated first (make enrollment table long
 * 							rather than wide) and THEN the final table is generated
 * 						
*/

/******************************
 * MEDICARE TEXAS
 ******************************/

--Medicare enrollment tables are wide rather than long, so let's fix that
--First, rearrange enrollment table into a long table
drop table if exists dw_staging.mcrt_member_enrollment_monthly_etl;

create table dw_staging.mcrt_member_enrollment_monthly_etl (
	year int2,
	bene_id text,
	dob date,
	dod date,
	sex text,
	state text,
	zip text,
	race text,
	month_year_id varchar(8),
	mdcr_status_code varchar(8),
	mdcr_entlmt_buyin_ind varchar(8),
	ptd_cntrct_id varchar(8)
)
distributed by (bene_id);

do $$
declare
	i int := 1;
	month char(2);
begin
	raise notice 'Elongating Medicare Texas enrollment table...';
	
	for i in 1..12
	loop
	    month := lpad(i::text, 2, '0');
		execute 'insert into dw_staging.mcrt_member_enrollment_monthly_etl
			select bene_enrollmt_ref_yr::int, bene_id,
				bene_birth_dt::date, bene_death_dt::date,
				sex_ident_cd, state_code, zip_cd, bene_race_cd,
				year || ''' || month || ''' as month_year_id,
				mdcr_status_code_' || month || ',
				mdcr_entlmt_buyin_ind_' || month || ',
				ptd_cntrct_id_' || month || '
			from medicare_texas.mbsf_abcd_summary
			where mdcr_status_code_' || month || ' in (''10'',''11'',''20'',''21'',''31'');'
					;
		raise notice 'Month % completed', month;
	end loop;

	raise notice 'Medicare Texas enrollment table elongation completed!';
end $$;

vacuum analyze dw_staging.mcrt_member_enrollment_monthly_etl;

--select * from dw_staging.mcrt_member_enrollment_monthly_etl order by year, bene_id limit 10;

/************
 * Make monthly enrollment table
 *   - Join in dim_uth_member_id
 *   - Join in ref_month_year (to calculate ages, etc)
 * 	 - Join in sex code
 *   - Join in race code
 *   - Join in state code
 * 	 - Join in entitlement buy-in table (determines plan_type)
 *   - Join in part D coverage (ptd - rx_coverage)
 */

drop table if exists dw_staging.mcrt_member_enrollment_monthly;

create table dw_staging.mcrt_member_enrollment_monthly 
(like data_warehouse.member_enrollment_monthly including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
;

insert into dw_staging.mcrt_member_enrollment_monthly (
	data_source, 
	year, 
	month_year_id, 
	uth_member_id,
	gender_cd, 
	state, 
	zip5, 
	zip3,
	age_cy, 
	dob_derived, 
	death_date,
	plan_type, 
	bus_cd, 
	rx_coverage, 
	fiscal_year, 
	race_cd,
	load_date,
	age_months,
	table_id_src,
	member_id_src,
	age_fy
	)		
select 'mcrt' as data_source,
	   a.year, 
	   c.month_year_id, 
	   b.uth_member_id,
	   d.gender_cd,
	   e.state_cd as state,
	   a.zip as zip5, 
	   substring(a.zip,1,3) as zip3,
  	   extract(years from age(c.cy_end, a.dob)) as age_cy,
	   a.dob as dob_derived, 
	   a.dod as death_date,
	   g.plan_type, 
	   null as bus_cd, 
	   case when h.ptd_coverage is null then 0 else h.ptd_coverage end as rx_coverage,
	   c.fy_ut as fiscal_year, 
	   f.race_cd,
	   current_date as load_date,
	   extract(years from age(to_date(a.month_year_id::text, 'YYYYMM'), a.dob)) * 12 + 
	   extract(months from age(to_date(a.month_year_id::text, 'YYYYMM'), a.dob)) as age_months,
	   'medicare_texas.mbsf_abcd_summary' as table_id_src,
	   a.bene_id as member_id_src,
  	   extract(years from age(c.fy_end, a.dob)) as age_fy
from dw_staging.mcrt_member_enrollment_monthly_etl a
  left join data_warehouse.dim_uth_member_id b
    on a.bene_id::text = b.member_id_src
   and b.data_source = 'mcrt'
  left join reference_tables.ref_month_year c
  	on a.month_year_id::int = c.month_year_id
  left join reference_tables.ref_gender d
    on d.data_source = 'mcr'
   and a.sex = d.gender_cd_src
  left join reference_tables.ref_medicare_state_codes e
     on a.state = e.medicare_state_cd
  left join reference_tables.ref_race f 
     on f.race_cd_src = a.race 
    and f.data_source = 'mcrt'
  left join reference_tables.ref_medicare_entlmt_buyin g 
    on g.buyin_cd = a.mdcr_entlmt_buyin_ind
  left join reference_tables.ref_medicare_ptd_cntrct h 
    on h.ptd_first_char = substring(a.ptd_cntrct_id,1,1)
;

analyze dw_staging.mcrt_member_enrollment_monthly;

/****************
 * CHECKPOINT: did the table joins generate a lot of nulls?
 */

select count(*), sum(case when uth_member_id is null then 1 else 0 end) as uth_memid,
	sum(case when month_year_id is null then 1 else 0 end) as monyr_id,
	sum(case when gender_cd is null then 1 else 0 end) as sex,
	sum(case when state is null then 1 else 0 end) as state,
	sum(case when race_cd is null then 1 else 0 end) as race,
	sum(case when plan_type is null then 1 else 0 end) as plan,
	sum(case when rx_coverage is null then 1 else 0 end) as rx_cvg,
	sum(case when uth_member_id is null then 1 else 0 end)*1.0/count(*) as uth_memid_pct,
	sum(case when month_year_id is null then 1 else 0 end)*1.0/count(*) as monyr_id_pct,
	sum(case when gender_cd is null then 1 else 0 end)*1.0/count(*) as sex_pct,
	sum(case when state is null then 1 else 0 end)*1.0/count(*) as state_pct,
	sum(case when race_cd is null then 1 else 0 end)*1.0/count(*) as race_pct,
	sum(case when plan_type is null then 1 else 0 end)*1.0/count(*) as plan_pct,
	sum(case when rx_coverage is null then 1 else 0 end)*1.0/count(*) as rx_cvg_pct
from dw_staging.mcrt_member_enrollment_monthly;

---**script to build consecutive enrolled months	
drop table if exists dw_staging.temp_mcrt_consec_enrollment;

create table dw_staging.temp_mcrt_consec_enrollment 
with (appendonly=true, orientation=column) as 
select month_year_id, uth_member_id
      ,row_number() over(partition by uth_member_id, my_grp order by  month_year_id) as in_streak
from ( 
	   select a.month_year_id,
	        a.uth_member_id,
	        b.my_row_counter - row_number() over(partition by a.uth_member_id
	        	order by a.month_year_id) as my_grp
	   from dw_staging.mcrt_member_enrollment_monthly a 
	     join reference_tables.ref_month_year b 
	       on a.month_year_id = b.month_year_id 	   		    
	 ) t    
distributed by (month_year_id);

analyze dw_staging.temp_mcrt_consec_enrollment;

--update consec enrolled months
update dw_staging.mcrt_member_enrollment_monthly a 
   set consecutive_enrolled_months = b.in_streak 
  from dw_staging.temp_mcrt_consec_enrollment b 
 where a.month_year_id = b.month_year_id and a.uth_member_id = b.uth_member_id;

--**cleanup
drop table if exists dw_staging.temp_mcrt_consec_enrollment;

--vacuum analyze
vacuum analyze dw_staging.mcrt_member_enrollment_monthly;








