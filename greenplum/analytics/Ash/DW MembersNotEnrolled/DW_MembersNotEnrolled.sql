--yearly level
drop table if exists dev.am_all_members_yearly;
select c.data_source , c.data_year , c.uth_member_id , c.member_id_src , date_part('month', c.from_date_of_service) as claim_month,
		date_part('year', c.from_date_of_service) as claim_year, e.uth_member_id as mem_not_enrolled
	into dev.am_all_members_yearly 
	from data_warehouse.claim_header c 
	left join data_warehouse.member_enrollment_yearly e on e.data_source = c.data_source and 
															e."year" = date_part('year', c.from_date_of_service) and 
															e.uth_member_id = c.uth_member_id 
-----------------------------------------------------------------------------------------------------------------------------------------------------							
--isolate members not enrolled
drop table if exists dev.am_members_not_enrolled;															
	select distinct c.data_source , claim_year , c.uth_member_id 
		into dev.am_members_not_enrolled
		from dev.am_all_members_yearly c
		where mem_not_enrolled is null
--select count(*) from dev.am_members_not_enrolled amne 
--1,248,701
-----------------------------------------------------------------------------------------------------------------------------------------------------							
--get source table from DW
drop table if exists dev.am_member_enrollment_yearly;

select * 
	into dev.am_member_enrollment_yearly 
	from data_warehouse.member_enrollment_yearly;

--drop row_identifier column
ALTER TABLE dev.am_member_enrollment_yearly DROP COLUMN row_identifier;
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
															
--1272358 new reocords inserted; 															
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
								
--update month flags
update dev.am_member_enrollment_yearly y
		set enrolled_jan = true
	from dev.am_member_enrollment_monthly_temp m 
	where claim_created_flag = true
		and y.uth_member_id = m.uth_member_id 
  		and y.year = m.claim_year 
 		and m.claim_month = 1;

update dev.am_member_enrollment_yearly y
		set enrolled_feb = true
	from dev.am_member_enrollment_monthly_temp m 
	where claim_created_flag = true
		and y.uth_member_id = m.uth_member_id 
  		and y.year = m.claim_year 
 		and m.claim_month = 2;	
	
update dev.am_member_enrollment_yearly y
		set enrolled_mar = true
	from dev.am_member_enrollment_monthly_temp m 
	where claim_created_flag = true
		and y.uth_member_id = m.uth_member_id 
  		and y.year = m.claim_year 
 		and m.claim_month = 3;	

update dev.am_member_enrollment_yearly y
		set enrolled_apr = true
	from dev.am_member_enrollment_monthly_temp m 
	where claim_created_flag = true
		and y.uth_member_id = m.uth_member_id 
  		and y.year = m.claim_year 
 		and m.claim_month = 4;	
 	
update dev.am_member_enrollment_yearly y
		set enrolled_may = true
	from dev.am_member_enrollment_monthly_temp m 
	where claim_created_flag = true
		and y.uth_member_id = m.uth_member_id 
  		and y.year = m.claim_year 
 		and m.claim_month = 5;	
 	
update dev.am_member_enrollment_yearly y
		set enrolled_jun = true
	from dev.am_member_enrollment_monthly_temp m 
	where claim_created_flag = true
		and y.uth_member_id = m.uth_member_id 
  		and y.year = m.claim_year 
 		and m.claim_month = 6;	 
 	
update dev.am_member_enrollment_yearly y
		set enrolled_jul = true
	from dev.am_member_enrollment_monthly_temp m 
	where claim_created_flag = true
		and y.uth_member_id = m.uth_member_id 
  		and y.year = m.claim_year 
 		and m.claim_month = 7;	 	
 	
 update dev.am_member_enrollment_yearly y
		set enrolled_aug = true
	from dev.am_member_enrollment_monthly_temp m 
	where claim_created_flag = true
		and y.uth_member_id = m.uth_member_id 
  		and y.year = m.claim_year 
 		and m.claim_month = 8;	 	
 
 update dev.am_member_enrollment_yearly y
		set enrolled_sep = true
	from dev.am_member_enrollment_monthly_temp m 
	where claim_created_flag = true
		and y.uth_member_id = m.uth_member_id 
  		and y.year = m.claim_year 
 		and m.claim_month = 9;	  	
 	
 update dev.am_member_enrollment_yearly y
		set enrolled_oct = true
	from dev.am_member_enrollment_monthly_temp m 
	where claim_created_flag = true
		and y.uth_member_id = m.uth_member_id 
  		and y.year = m.claim_year 
 		and m.claim_month = 10;	 	
 	
 update dev.am_member_enrollment_yearly y
		set enrolled_nov = true
	from dev.am_member_enrollment_monthly_temp m 
	where claim_created_flag = true
		and y.uth_member_id = m.uth_member_id 
  		and y.year = m.claim_year 
 		and m.claim_month = 11;	
 	
 update dev.am_member_enrollment_yearly y
		set enrolled_dec = true
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
drop table if exists dev.am_member_enrollment_monthly_temp
 	
vacuum analyze dev.am_member_enrollment_yearly;	


/*											
select uth_member_id , month_year_id  
	from dev.am_member_enrollment_monthly
	group by uth_member_id , month_year_id 
	having  count(month_year_id ) > 1


 
						
							
-----------------------------------------------------------------------------------------------------------------------------------------------------							
 
	
	
		
	 
	
 	
 
	
	
select *
   from dev.am_member_enrollment_monthly 	 a 		     
   where 
--clean up like 100001393 <=> 201712, 100000311 <=> 201112 multiple entires
	
 
	
	
		
 
			
			
	







															
				
															
															
															
															
	
select distinct data_source , data_year , uth_member_id , member_id_src 
	from dev.am_mem_not_enrolled 
	where en_mem is null
		and data_source = 'optd'	
	
															
select data_year , data_source , count(distinct uth_member_id) uth_member_id_Count  
	from dev.am_mem_not_enrolled 
	where en_mem is null
		and data_source = 'optd'
		and data_year = 2007
		and uth_member_id not in (select uth_member_id from data_warehouse.member_enrollment_yearly mey where mey.data_source ='optd' )
	group by data_year , data_source 
	order by data_source,  data_year  	
		
	
select e.uth_member_id 
	from dev.am_mem_not_enrolled e
	inner join optum_dod.mbr_co_enroll_r r on cast(r.patid as text) = e.member_id_src 
	where en_mem is null
		and data_source = 'optd'
		and to_date(e.data_year::varchar, 'yyyy') between r.eligeff and r.eligend 

		
select distinct ch.claim_type 
	from data_warehouse.claim_header ch 
	
	
	
	 
 select l.*, e.* into dev.am_temp from
(select data_source, year, uth_member_id from data_warehouse.claim_header ) l left join

(select distinct data_source ds, year yr, uth_member_id mem from data_warehouse.member_enrollment_yearly) e on 
l.data_source = e.ds and l.year= e.yr and l.uth_member_id = e.mem
*/


	
	
	
	
