drop table if exists dev.am_opioid_raw_stats;

--Step 1a: get continuous enrolment in 2012
drop table if exists dev.am_opt_opioid_enrolled_2012;
	select distinct uth_member_id, count(year) as total_enrolled_months
		into dev.am_opt_opioid_enrolled_2012
		from data_warehouse.member_enrollment_monthly mem 
		where /*rx_coverage = 1	
			and */data_source ='optz'
			and year = 2012
		group by uth_member_id , data_source, rx_coverage, year
		having count(year) = 12
		order by uth_member_id ;

select 1 as line_no, 'Step 1a: get continuous enrolment in 2012' description, count(*) total 
	into dev.am_opioid_raw_stats
	from dev.am_opt_opioid_enrolled_2012;	
--10679879 --old
	

--Step 1b: get first time opioid prescription of members continuously enrolled in 2012
drop table if exists dev.am_opt_opioid_step_1;
	select p.uth_member_id , min(fill_date)	first_fill_date
		into dev.am_opt_opioid_step_1
		FROM data_warehouse.pharmacy_claims p
		inner join dev.am_opt_opioid_enrolled_2012 m on m.uth_member_id = p.uth_member_id 
		inner join dev.am_opioid_ndc n on n.ndc = p.ndc 
		where p.year =2012 
			and p.data_source = 'optz'
		group by p.uth_member_id;

insert into dev.am_opioid_raw_stats
select 2 as line_no, 'Step 1b: get first time opioid prescription of members continuously enrolled in 2012' description, count(*) total 	
	from dev.am_opt_opioid_step_1;
--1508332--old
-------------------------------------------------------------------------------------------------------	
--Step 2: inclusion: who had consecutive enrollment for the next 5 years from 2013-2017
drop table if exists dev.am_opt_opioid_enrolled_from_2013_to_2017_raw;

	select m.uth_member_id, year, count(year) as total_enrolled_months
		into dev.am_opt_opioid_enrolled_from_2013_to_2017_raw
		from data_warehouse.member_enrollment_monthly m, dev.am_opt_opioid_step_1 s1 
		where m.uth_member_id = s1.uth_member_id 
			and	m.rx_coverage = 1
			and m.data_source ='optz'
			and m.year = 2013
		group by m.uth_member_id , year
		having count(year) = 12
		order by uth_member_id ;
	
	insert into dev.am_opt_opioid_enrolled_from_2013_to_2017_raw( uth_member_id, year, total_enrolled_months)	
	select m.uth_member_id, year, count(year) as total_enrolled_months		
		from data_warehouse.member_enrollment_monthly m, dev.am_opt_opioid_step_1 s1 
		where m.uth_member_id = s1.uth_member_id 
			and	m.rx_coverage = 1
			and m.data_source ='optz'
			and m.year = 2014
		group by m.uth_member_id , year
		having count(year) = 12
		order by uth_member_id ;
	
	insert into dev.am_opt_opioid_enrolled_from_2013_to_2017_raw( uth_member_id, year, total_enrolled_months)	
	select m.uth_member_id, year, count(year) as total_enrolled_months		
		from data_warehouse.member_enrollment_monthly m, dev.am_opt_opioid_step_1 s1 
		where m.uth_member_id = s1.uth_member_id 
			and	m.rx_coverage = 1
			and m.data_source ='optz'
			and m.year = 2015
		group by m.uth_member_id , year
		having count(year) = 12
		order by uth_member_id ;
	
	insert into dev.am_opt_opioid_enrolled_from_2013_to_2017_raw( uth_member_id, year, total_enrolled_months)	
	select m.uth_member_id, year, count(year) as total_enrolled_months		
		from data_warehouse.member_enrollment_monthly m, dev.am_opt_opioid_step_1 s1 
		where m.uth_member_id = s1.uth_member_id 
			and	m.rx_coverage = 1
			and m.data_source ='optz'
			and m.year = 2016
		group by m.uth_member_id , year
		having count(year) = 12
		order by uth_member_id ;
	
	insert into dev.am_opt_opioid_enrolled_from_2013_to_2017_raw( uth_member_id, year, total_enrolled_months)	
	select m.uth_member_id, year, count(year) as total_enrolled_months		
		from data_warehouse.member_enrollment_monthly m, dev.am_opt_opioid_step_1 s1 
		where m.uth_member_id = s1.uth_member_id 
			and	m.rx_coverage = 1
			and m.data_source ='optz'
			and m.year = 2017
		group by m.uth_member_id , year
		having count(year) = 12
		order by uth_member_id ;
	
drop table if exists dev.am_opt_opioid_step_2;
	select distinct uth_member_id
		into dev.am_opt_opioid_step_2
		from dev.am_opt_opioid_enrolled_from_2013_to_2017_raw		
		group by uth_member_id  	
		having count(year) = 5;

insert into dev.am_opioid_raw_stats
select 3 as line_no, 'Step 2: inclusion: who had consecutive enrollment for the next 5 years from 2013-2017' description, count(*) total 	
	from dev.am_opt_opioid_step_2;
--1,508,332--old


select * from dev.am_opioid_raw_stats order by line_no





-------------------------------------------------------------------------------------------------------
--Step 3: inclusion: who had pharmacy claims in 2012, meet the opioid criteria by pharma codes, using OPIOID NDC code (2018) with a total of 90 days or more supply
drop table if exists dev.am_opt_opioid_step_3;
	select distinct p.uth_member_id 
		into dev.am_opt_opioid_step_3
		FROM data_warehouse.pharmacy_claims p
		inner join dev.am_opt_opioid_step_2 m on m.uth_member_id = p.uth_member_id 
		inner join dev.am_opioid_ndc n on n.ndc = p.ndc 
		where p.year =2012 
			and p.data_source = 'optz'
			and p.days_supply >= 90;

--select count(*) as Step_3_Count from dev.am_opt_opioid_step_3
--9587	
-------------------------------------------------------------------------------------------------------		
--Step 4: exclusion: who had opioid prescription (claims by NDC codes) from 9/1/2011 – 12/31/2011 
drop table if exists dev.am_opt_opioid_step_4a;			

	select distinct p.uth_member_id  
		into dev.am_opt_opioid_step_4a
		FROM data_warehouse.pharmacy_claims p
		inner join dev.am_opt_opioid_step_3 m on m.uth_member_id = p.uth_member_id 
		inner join dev.am_opioid_ndc n on n.ndc = p.ndc 
		where p.data_source = 'optz'
			and p.fill_date between '9/1/2011' and  '12/31/2011'; 

--select count(*) as Step_4a_Count from dev.am_opt_opioid_step_4a
--6582				
			
drop table if exists dev.am_opt_opioid_step_4;	

	select distinct uth_member_id
		into dev.am_opt_opioid_step_4
		from dev.am_opt_opioid_step_3
		where uth_member_id not in (select uth_member_id from dev.am_opt_opioid_step_4a);
		
--select count(*) as Step_4_Count from dev.am_opt_opioid_step_4		
--3005		
-------------------------------------------------------------------------------------------------------		
--Step 5: exclusion: who had cancer diagnosis (from inpatient and outpatient) by the cancer DX icd9 codes, period:  1/1/2012-12/31/2012
drop table if exists dev.am_opt_opioid_step_5a;		
		
	select distinct m.uth_member_id  
		into dev.am_opt_opioid_step_5a
		FROM data_warehouse.claim_diag d
		inner join dev.am_opt_opioid_step_4 m on m.uth_member_id = d.uth_member_id 
		inner join dev.am_opioid_cancer c on c.dx = d.diag_cd 
		where d.data_source = 'optz'
			and d.year = 2012;

--select count(*) as Step_5a_Count from dev.am_opt_opioid_step_5a
--90							

drop table if exists dev.am_opt_opioid_step_5;	

	select distinct uth_member_id
		into dev.am_opt_opioid_step_5
		from dev.am_opt_opioid_step_4
		where uth_member_id not in (select uth_member_id from dev.am_opt_opioid_step_5a);
					
--select count(*) as Step_5_Count from dev.am_opt_opioid_step_5			
--2915			
-------------------------------------------------------------------------------------------------------		
--Step 6: exclusion: who had a cognitive impairment diagnosis prior to 2012 , by DX icd9 codes, period: no beginning date - 12/31/2011
drop table if exists dev.am_opt_opioid_step_6a;		
	
	select distinct m.uth_member_id  
		into dev.am_opt_opioid_step_6a
		from data_warehouse.claim_diag d
		inner join dev.am_opt_opioid_step_5 m on m.uth_member_id = d.uth_member_id 		
		where d.data_source = 'optz'		
			and d.from_date_of_service <= '2011-12-31' 
			and (
					diag_cd in ('29040','29041', '29042', '29043','29282')
					or 
					(diag_cd like '331%' or diag_cd like '2941%' or diag_cd like '2942%' or diag_cd like '2948%' or diag_cd like '2949%')
				);
	
--select count(*) as Step_6a_Count from dev.am_opt_opioid_step_6a			
--95

drop table if exists dev.am_opt_opioid_step_6;	

	select distinct uth_member_id
		into dev.am_opt_opioid_step_6
		from dev.am_opt_opioid_step_5
		where uth_member_id not in (select uth_member_id from dev.am_opt_opioid_step_6a);
					
--select count(*) as Step_6_Count from dev.am_opt_opioid_step_6				
--2820	
-------------------------------------------------------------------------------------------------------
--get member_id_src and bus_code	
drop table if exists dev.am_optz_opioid_members;
	select distinct d.uth_member_id, d.member_id_src, d.data_source, e.bus_cd
		into dev.am_optz_opioid_members
		from dev.am_opt_opioid_step_6 a, data_warehouse.dim_uth_member_id d, data_warehouse.member_enrollment_yearly e	
		where a.uth_member_id =d.uth_member_id 
			and a.uth_member_id = e.uth_member_id;
-------------------------------------------------------------------------------------------------------
--get all MCR data
-------------------------------------------------------------------------------------------------------

--confinement
drop table if exists dev.am_optz_opioid_confinement_mcr;
select distinct * 
	into dev.am_optz_opioid_confinement_mcr
	from optum_zip.confinement c, dev.am_optz_opioid_members m
	where c.patid::text = m.member_id_src
		and m.bus_cd = 'MCR'
		and c.year >= 2012; 
 	 
--diagnostic
drop table if exists dev.am_optz_opioid_diagnostic_mcr;
select distinct * 
	into dev.am_optz_opioid_diagnostic_mcr
	from optum_zip.diagnostic c, dev.am_optz_opioid_members m
	where c.patid::text = m.member_id_src
		and m.bus_cd = 'MCR'
		and c.year >= 2012; 
	 
--medical
drop table if exists  dev.am_optz_opioid_medical_mcr;
select distinct * 
	into dev.am_optz_opioid_medical_mcr
	from optum_zip.medical c, dev.am_optz_opioid_members m
		where c.patid::text = m.member_id_src
			and m.bus_cd = 'MCR'
			and c.year >= 2012; 
	
--procedure
drop table if exists  dev.am_optz_opioid_procedure_mcr;
select distinct * 
	into dev.am_optz_opioid_procedure_mcr
	from optum_zip."procedure" c, dev.am_optz_opioid_members m
	where c.patid::text = m.member_id_src
		and m.bus_cd = 'MCR'
		and c.year >= 2012; 	
	
--RX
drop table if exists  dev.am_optz_opioid_rx_mcr;
select distinct * 
	into dev.am_optz_opioid_rx_mcr
	from optum_zip.rx c, dev.am_optz_opioid_members m
	where c.patid::text = m.member_id_src
		and m.bus_cd = 'MCR'
		and c.year >= 2012; 	
		
-------------------------------------------------------------------------------------------------------
--get all COM data
-------------------------------------------------------------------------------------------------------

--confinement
drop table if exists dev.am_optz_opioid_confinement_com;
select distinct * 
	into dev.am_optz_opioid_confinement_com
	from optum_zip.confinement c, dev.am_optz_opioid_members m
	where c.patid::text = m.member_id_src
		and m.bus_cd = 'COM'
		and c.year >= 2012; 
 	 
--diagnostic
drop table if exists dev.am_optz_opioid_diagnostic_com;
select distinct * 
	into dev.am_optz_opioid_diagnostic_com
	from optum_zip.diagnostic c, dev.am_optz_opioid_members m
	where c.patid::text = m.member_id_src
		and m.bus_cd = 'COM'
		and c.year >= 2012; 
	 
--medical
drop table if exists  dev.am_optz_opioid_medical_com;
select distinct * 
	into dev.am_optz_opioid_medical_com
	from optum_zip.medical c, dev.am_optz_opioid_members m
		where c.patid::text = m.member_id_src
			and m.bus_cd = 'COM'
			and c.year >= 2012; 
	
--procedure
drop table if exists  dev.am_optz_opioid_procedure_com;
select distinct * 
	into dev.am_optz_opioid_procedure_com
	from optum_zip."procedure" c, dev.am_optz_opioid_members m
	where c.patid::text = m.member_id_src
		and m.bus_cd = 'COM'
		and c.year >= 2012; 	
	
--RX
drop table if exists  dev.am_optz_opioid_rx_com;
select distinct * 
	into dev.am_optz_opioid_rx_com
	from optum_zip.rx c, dev.am_optz_opioid_members m
	where c.patid::text = m.member_id_src
		and m.bus_cd = 'COM'
		and c.year >= 2012; 	
			
		
	
	
	
	
	
	
	
	
	
	
	
	
		
/*		
select count(distinct o.uth_member_id )
from dev.am_opt_opioid_step_6 o, data_warehouse.member_enrollment_yearly e
where o.uth_member_id = e.uth_member_id 
	and e.bus_cd = 'MCR'
--2471	
	
select count(distinct o.uth_member_id ) 
from dev.am_opt_opioid_step_6 o, data_warehouse.member_enrollment_yearly e
where o.uth_member_id = e.uth_member_id 
	and e.bus_cd = 'COM'	
--478	
	
	
	
	
	
select count(distinct o.uth_member_id ), m."year" 
	from dev.am_opt_opioid_step_6 o,data_warehouse.member_enrollment_monthly m
	where m.uth_member_id = o.uth_member_id 
		and m."year" in (2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019)
	group by 2
	order by 2
		
select count(distinct o.uth_member_id ), date_part('year', e.eligeff)
	from dev.am_opt_opioid_step_6 o, data_warehouse.dim_uth_member_id d, optum_zip.mbr_enroll e
	where o.uth_member_id = d.uth_member_id 
		and d.member_id_src = cast(e.patid as text)		
	group by 2
	order by 2
*/	
	
	
	
 