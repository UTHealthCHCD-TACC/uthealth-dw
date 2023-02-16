--yearly level
drop table if exists dev.am_all_members_yearly;
create table dev.am_all_members_yearly 
with(appendonly=true,orientation=column)
as
select c.data_source , c.year , c.uth_member_id , c.member_id_src , date_part('month', c.from_date_of_service) as claim_month,
		date_part('year', c.from_date_of_service) as claim_year, e.uth_member_id as mem_not_enrolled
	from data_warehouse.claim_header c 
	left join data_warehouse.member_enrollment_yearly e on e.data_source = c.data_source and 
															e."year" = date_part('year', c.from_date_of_service) and 
															e.uth_member_id = c.uth_member_id
distributed by (uth_member_id);

-----------------------------------------------------------------------------------------------------------------------------------------------------							
--isolate members not enrolled
drop table if exists dev.am_members_not_enrolled;	
create table dev.am_members_not_enrolled 
with(appendonly=true,orientation=column)
as
	select distinct c.data_source , claim_year , c.uth_member_id 
		from dev.am_all_members_yearly c
		where mem_not_enrolled is null
distributed by (uth_member_id);
--select count(*) from dev.am_members_not_enrolled amne 
--1,248,701
-----------------------------------------------------------------------------------------------------------------------------------------------------							
--get source table from DW
drop table if exists dev.am_member_enrollment_yearly;
create table dev.am_member_enrollment_yearly 
with(appendonly=true,orientation=column)
as
select * 
	from data_warehouse.member_enrollment_yearly
distributed BY (uth_member_id);

--drop row_identifier column
--ALTER TABLE dev.am_member_enrollment_yearly DROP COLUMN row_identifier;
-----------------------------------------------------------------------------------------------------------------------------------------------------		
--following will create additional records of uth_member_id already exists in enrollment table
--we use it to get gender, age, dob, rx and race															
insert into dev.am_member_enrollment_yearly (data_source, year, uth_member_id, gender_cd, age_derived, 
											 dob_derived, death_date, claim_created_flag, rx_coverage, race_cd )													 
	select distinct y.data_source, n.claim_year , y.uth_member_id , gender_cd, 
			case when death_date is not null then extract(year from death_date) else n.claim_year end - extract(year from dob_derived) as age_derived,
			dob_derived, death_date, true as claim_created_flag , rx_coverage , race_cd			
		FROM dev.am_member_enrollment_yearly y, dev.am_members_not_enrolled n
		where y.uth_member_id = n.uth_member_id 	
			and y.data_source = n.data_source 			
			and not exists (select uth_member_id 
								from dev.am_member_enrollment_yearly m
								where m.uth_member_id = y.uth_member_id
									and m.data_source = y.data_source 
									and m."year" = n.claim_year );
															
--1,272,358 new reocords inserted; 															
--select  count(*) from dev.am_member_enrollment_yearly ch where claim_created_flag = true
-----------------------------------------------------------------------------------------------------------------------------------------------------	
drop table if exists dev.am_member_enrollment_monthly_temp;
create table dev.am_member_enrollment_monthly_temp
	with (appendonly=true, orientation=column)
	as
	select distinct data_source , claim_year , uth_member_id ,claim_month, cast(concat(claim_year, LPAD(claim_month::text, 2,'0')) as int4) as month_year_id 
		from dev.am_all_members_yearly
		where mem_not_enrolled is null
		distributed by(uth_member_id);								
	
--reset all values to false
update dev.am_member_enrollment_yearly 
		set enrolled_jan = 0,
			enrolled_feb = 0,
			enrolled_mar = 0,
			enrolled_apr = 0,
			enrolled_may = 0,
			enrolled_jun = 0,
			enrolled_jul = 0,
			enrolled_aug = 0,
			enrolled_sep = 0,
			enrolled_oct = 0,
			enrolled_nov = 0,
			enrolled_dec = 0
	where claim_created_flag = true;
		
--update month flags
update dev.am_member_enrollment_yearly y
		set enrolled_jan = 1
	from dev.am_member_enrollment_monthly_temp m 
	where claim_created_flag = true
		and y.uth_member_id = m.uth_member_id 
  		and y.year = m.claim_year 
 		and m.claim_month = 1;

update dev.am_member_enrollment_yearly y
		set enrolled_feb = 1
	from dev.am_member_enrollment_monthly_temp m 
	where claim_created_flag = true
		and y.uth_member_id = m.uth_member_id 
  		and y.year = m.claim_year 
 		and m.claim_month = 2;	
	
update dev.am_member_enrollment_yearly y
		set enrolled_mar = 1
	from dev.am_member_enrollment_monthly_temp m 
	where claim_created_flag = true
		and y.uth_member_id = m.uth_member_id 
  		and y.year = m.claim_year 
 		and m.claim_month = 3;	

update dev.am_member_enrollment_yearly y
		set enrolled_apr = 1
	from dev.am_member_enrollment_monthly_temp m 
	where claim_created_flag = true
		and y.uth_member_id = m.uth_member_id 
  		and y.year = m.claim_year 
 		and m.claim_month = 4;	
 	
update dev.am_member_enrollment_yearly y
		set enrolled_may = 1
	from dev.am_member_enrollment_monthly_temp m 
	where claim_created_flag = true
		and y.uth_member_id = m.uth_member_id 
  		and y.year = m.claim_year 
 		and m.claim_month = 5;	
 	
update dev.am_member_enrollment_yearly y
		set enrolled_jun = 1
	from dev.am_member_enrollment_monthly_temp m 
	where claim_created_flag = true
		and y.uth_member_id = m.uth_member_id 
  		and y.year = m.claim_year 
 		and m.claim_month = 6;	 
 	
update dev.am_member_enrollment_yearly y
		set enrolled_jul = 1
	from dev.am_member_enrollment_monthly_temp m 
	where claim_created_flag = true
		and y.uth_member_id = m.uth_member_id 
  		and y.year = m.claim_year 
 		and m.claim_month = 7;	 	
 	
 update dev.am_member_enrollment_yearly y
		set enrolled_aug = 1
	from dev.am_member_enrollment_monthly_temp m 
	where claim_created_flag = true
		and y.uth_member_id = m.uth_member_id 
  		and y.year = m.claim_year 
 		and m.claim_month = 8;	 	
 
 update dev.am_member_enrollment_yearly y
		set enrolled_sep = 1
	from dev.am_member_enrollment_monthly_temp m 
	where claim_created_flag = true
		and y.uth_member_id = m.uth_member_id 
  		and y.year = m.claim_year 
 		and m.claim_month = 9;	  	
 	
 update dev.am_member_enrollment_yearly y
		set enrolled_oct = 1
	from dev.am_member_enrollment_monthly_temp m 
	where claim_created_flag = true
		and y.uth_member_id = m.uth_member_id 
  		and y.year = m.claim_year 
 		and m.claim_month = 10;	 	
 	
 update dev.am_member_enrollment_yearly y
		set enrolled_nov = 1
	from dev.am_member_enrollment_monthly_temp m 
	where claim_created_flag = true
		and y.uth_member_id = m.uth_member_id 
  		and y.year = m.claim_year 
 		and m.claim_month = 11;	
 	
 update dev.am_member_enrollment_yearly y
		set enrolled_dec = 1
	from dev.am_member_enrollment_monthly_temp m 
	where claim_created_flag = true
		and y.uth_member_id = m.uth_member_id 
  		and y.year = m.claim_year 
 		and m.claim_month = 12;	 	
 	
 ------------------------------------------------------------------------------------------------------------------------------------------------------
--calculate total enrolled months
update dev.am_member_enrollment_yearly
	set total_enrolled_months=enrolled_jan::int+enrolled_feb::int+enrolled_mar::int+enrolled_apr::int+enrolled_may::int+enrolled_jun::int+enrolled_jul::int+enrolled_aug::int+enrolled_sep::int+enrolled_oct::int+enrolled_nov::int+enrolled_dec::int
	where claim_created_flag = true;
	
------------------------------------------------------------------------------------------------------------------------------------------------------
drop table if exists dev.am_member_enrollment_monthly_temp;
drop table if exists dev.am_all_members_yearly;
drop table if exists dev.am_members_not_enrolled;
------------------------------------------------------------------------------------------------------------------------------------------------------
vacuum analyze dev.am_member_enrollment_yearly;

select * 
	from dev.am_member_enrollment_yearly
	where claim_created_flag = true;

select count(*) 
	from dev.am_member_enrollment_yearly
	where claim_created_flag = true;


	
	
	
	
