/*
create table dev.am_member_race (data_source, member_id_src, race, uth_member_id, data_year)
 
create table dev.am_member_enrollment_yearly
	with(appendonly=true,orientation=column,compresstype=zlib)
	as select * from data_warehouse.member_enrollment_yearly
	distributed by(uth_member_id) 
	
delete from dev.am_member_race
*/
-----------------------------------------------------------------------------------------------------------
--MCRN
insert into dev.am_member_race ( data_source , member_id_src , race , data_year )
	select distinct 'mcrn' as data_source, bene_id as member_id_src, cast(bene_race_cd as int) as race ,cast(year as int)
		from medicare_national.mbsf_abcd_summary  
		where bene_id not in (select member_id_src from dev.am_member_race where data_source = 'mcrn')
-----------------------------------------------------------------------------------------------------------		
--MCRT
insert into dev.am_member_race ( data_source , member_id_src , race , data_year )
	select distinct 'mcrt' as data_source, bene_id as member_id_src, cast(bene_race_cd as int) as race ,cast(year as int)
		from medicare_texas.mbsf_abcd_summary  
		where bene_id not in (select member_id_src from dev.am_member_race where data_source = 'mcrt')
 	 
-------------------------------------------------------------------------------------------		
--OPTD
insert into dev.am_member_race ( data_source ,  member_id_src , race )	
	select distinct 'optd' as data_source,  patid as member_id_src, race as race
		from optum_zip.mbr_enroll_r mer 
		where race is not null
-------------------------------------------------------------------------------------------				
--get uth_member_id		
update dev.am_member_race r
		set uth_member_id  = m.uth_member_id
	from data_warehouse.dim_uth_member_id m
	where m.member_id_src = r.member_id_src 
		and m.data_source = r.data_source 		
------------------------------------------------------------------------------------------		
--duplicate record check		
select count(uth_member_id)	as uthCount, uth_member_id, data_source , data_year 
	from dev.am_member_race amr 
	--where data_source = 'optd'
	group by uth_member_id, data_source , data_year 
	having count(uth_member_id ) > 1			
------------------------------------------------------------------------------------------
	
select count(distinct member_id_src)
	from dev.am_member_race
--62,362,421

------------------------------------------------------------------------------------------		
--main update 
update dev.am_member_enrollment_yearly y
	set race = 		case 
						when r.data_source = 'mcrn' and r.race = '1' then '1' -- White  
						when r.data_source = 'mcrn' and r.race = '2' then '2' -- Black
						when r.data_source = 'mcrn' and r.race = '3' then '3' -- Other 
						when r.data_source = 'mcrn' and r.race = '4' then '4' -- Asian 
						when r.data_source = 'mcrn' and r.race = '5' then '5' -- Hispanic
						when r.data_source = 'mcrn' and r.race = '6' then '6' -- North American Native					
						when r.data_source = 'mcrt' and r.race = '1' then '1' 
						when r.data_source = 'mcrt' and r.race = '2' then '2' 
						when r.data_source = 'mcrt' and r.race = '3' then '3' 
						when r.data_source = 'mcrt' and r.race = '4' then '4' 
						when r.data_source = 'mcrt' and r.race = '5' then '5' 
						when r.data_source = 'mcrt' and r.race = '6' then '6' 						
						when r.data_source = 'optd' and r.race = 'W' then '1' 
						when r.data_source = 'optd' and r.race = 'B' then '2' 
						when r.data_source = 'optd' and r.race = 'H' then '5' 
						when r.data_source = 'optd' and r.race = 'A' then '4'
						else '0' 										  -- Not provided
					end 
	from dev.am_member_race r 
	where r.uth_member_id = y.uth_member_id
		and r.data_source = y.data_source
		and r.data_year  = y.year
	
update	dev.am_member_enrollment_yearly
	set race = '0' -- Not provided
	where race is null
		and data_source in ('mcrn', 'mcrt', 'optd')
-------------------------------------------------------------------------------------------	
select race, *
	from dev.am_member_enrollment_yearly 
	where  data_source in ('mcrn', 'mcrt', 'optd')
	
select count(*)
		from dev.am_member_enrollment_yearly amey 
--896326938	

select count(*) 
	from data_warehouse.member_enrollment_yearly mey 
--896326938	 
 
	
	 
 



