drop table if exists dev.am_opioid_raw_stats;

--Step 1a: get continuous enrolment in 2012
drop table if exists dev.am_opt_opioid_enrolled_2012;
	select distinct uth_member_id, y.total_enrolled_months 
		into dev.am_opt_opioid_enrolled_2012
		from data_warehouse.member_enrollment_yearly y 
		where rx_coverage = 1	
			and data_source ='optz'
			and year = 2012
			and y.total_enrolled_months = 12
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

	select distinct m.uth_member_id, year, total_enrolled_months
		into dev.am_opt_opioid_enrolled_from_2013_to_2017_raw
		from data_warehouse.member_enrollment_yearly m, dev.am_opt_opioid_step_1 s1 
		where m.uth_member_id = s1.uth_member_id 
			and	m.rx_coverage = 1
			and m.data_source ='optz'
			and m.year = 2013
			and total_enrolled_months = 12;
			
	insert into dev.am_opt_opioid_enrolled_from_2013_to_2017_raw( uth_member_id, year, total_enrolled_months)	
		select distinct m.uth_member_id, year, total_enrolled_months			
			from data_warehouse.member_enrollment_yearly m, dev.am_opt_opioid_step_1 s1 
			where m.uth_member_id = s1.uth_member_id 
				and	m.rx_coverage = 1
				and m.data_source ='optz'
				and m.year = 2014
				and total_enrolled_months = 12;
		
	insert into dev.am_opt_opioid_enrolled_from_2013_to_2017_raw( uth_member_id, year, total_enrolled_months)	
		select distinct m.uth_member_id, year, total_enrolled_months			
			from data_warehouse.member_enrollment_yearly m, dev.am_opt_opioid_step_1 s1 
			where m.uth_member_id = s1.uth_member_id 
				and	m.rx_coverage = 1
				and m.data_source ='optz'
				and m.year = 2015
				and total_enrolled_months = 12;
	
	insert into dev.am_opt_opioid_enrolled_from_2013_to_2017_raw( uth_member_id, year, total_enrolled_months)	
		select distinct m.uth_member_id, year, total_enrolled_months			
			from data_warehouse.member_enrollment_yearly m, dev.am_opt_opioid_step_1 s1 
			where m.uth_member_id = s1.uth_member_id 
				and	m.rx_coverage = 1
				and m.data_source ='optz'
				and m.year = 2016
				and total_enrolled_months = 12;
	
	insert into dev.am_opt_opioid_enrolled_from_2013_to_2017_raw( uth_member_id, year, total_enrolled_months)	
		select distinct m.uth_member_id, year, total_enrolled_months			
			from data_warehouse.member_enrollment_yearly m, dev.am_opt_opioid_step_1 s1 
			where m.uth_member_id = s1.uth_member_id 
				and	m.rx_coverage = 1
				and m.data_source ='optz'
				and m.year = 2017
				and total_enrolled_months = 12;
	
 	
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

insert into dev.am_opioid_raw_stats
	select 4 as line_no, 'Step 3: inclusion: who had pharmacy claims in 2012, meet the opioid criteria by pharma codes, using OPIOID NDC code (2018) with a total of 90 days or more supply' description, count(*) total 	
		from dev.am_opt_opioid_step_3;
--9587	--old
				
		
-------------------------------------------------------------------------------------------------------		
--Step 4: exclusion: who had opioid prescription (claims by NDC codes) from 9/1/2011 � 12/31/2011 
drop table if exists dev.am_opt_opioid_step_4a;			

	select distinct p.uth_member_id  
		into dev.am_opt_opioid_step_4a
		FROM data_warehouse.pharmacy_claims p
		inner join dev.am_opt_opioid_step_3 m on m.uth_member_id = p.uth_member_id 
		inner join dev.am_opioid_ndc n on n.ndc = p.ndc 
		where p.data_source = 'optz'
			and p.fill_date between '9/1/2011' and  '12/31/2011'; 

insert into dev.am_opioid_raw_stats
	select 5 as line_no, 'Step 4a: exclusion: who had opioid prescription (claims by NDC codes) from 9/1/2011 � 12/31/2011' description, count(*) total 	
		from dev.am_opt_opioid_step_4a;
--6582 -- old				
			
drop table if exists dev.am_opt_opioid_step_4;	

	select distinct uth_member_id
		into dev.am_opt_opioid_step_4
		from dev.am_opt_opioid_step_3
		where uth_member_id not in (select uth_member_id from dev.am_opt_opioid_step_4a);

insert into dev.am_opioid_raw_stats
	select 6 as line_no, 'Step 4: exclusion: who had opioid prescription (claims by NDC codes) from 9/1/2011 � 12/31/2011' description, count(*) total 	
		from dev.am_opt_opioid_step_4;		
--3005	-- old	
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

insert into dev.am_opioid_raw_stats
	select 7 as line_no, 'Step 5a: exclusion: who had cancer diagnosis (from inpatient and outpatient) by the cancer DX icd9 codes, period:  1/1/2012-12/31/2012' description, count(*) total 	
		from dev.am_opt_opioid_step_5a;		
--90  --old							

drop table if exists dev.am_opt_opioid_step_5;	

	select distinct uth_member_id
		into dev.am_opt_opioid_step_5
		from dev.am_opt_opioid_step_4
		where uth_member_id not in (select uth_member_id from dev.am_opt_opioid_step_5a);
	
insert into dev.am_opioid_raw_stats
	select 8 as line_no, 'Step 5: exclusion: who had cancer diagnosis (from inpatient and outpatient) by the cancer DX icd9 codes, period:  1/1/2012-12/31/2012' description, count(*) total 	
		from dev.am_opt_opioid_step_5;	
--2915	--old
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

insert into dev.am_opioid_raw_stats
	select 9 as line_no, 'Step 6a: exclusion: who had a cognitive impairment diagnosis prior to 2012 , by DX icd9 codes, period: no beginning date - 12/31/2011' description, count(*) total 	
		from dev.am_opt_opioid_step_6a;							
--95 -old

drop table if exists dev.am_opt_opioid_step_6;
	select distinct uth_member_id
		into dev.am_opt_opioid_step_6
		from dev.am_opt_opioid_step_5
		where uth_member_id not in (select uth_member_id from dev.am_opt_opioid_step_6a);
	
insert into dev.am_opioid_raw_stats
	select 10 as line_no, 'Step 6: exclusion: who had a cognitive impairment diagnosis prior to 2012 , by DX icd9 codes, period: no beginning date - 12/31/2011' description, count(*) total 	
		from dev.am_opt_opioid_step_6;	
			
--2820	--old
-------------------------------------------------------------------------------------------------------
--get member_id_src and bus_code	
drop table if exists dev.am_optz_opioid_members;
	select distinct d.uth_member_id, d.member_id_src, d.data_source, e.bus_cd
		into dev.am_optz_opioid_members
		from dev.am_opt_opioid_step_6 a, data_warehouse.dim_uth_member_id d, data_warehouse.member_enrollment_yearly e	
		where a.uth_member_id =d.uth_member_id 
			and a.uth_member_id = e.uth_member_id;	

insert into dev.am_opioid_raw_stats	
	select 11 as line_no, 'MCR' description, count(distinct uth_member_id ) total
		from dev.am_optz_opioid_members
		where bus_cd = 'MCR'
		group by bus_cd ;
--2471	--old
insert into dev.am_opioid_raw_stats		
	select 12 as line_no, 'COM' description, count(distinct uth_member_id ) total
		from dev.am_optz_opioid_members
		where bus_cd = 'COM'
		group by bus_cd ;
--478	--old		
		
select * from dev.am_opioid_raw_stats order by line_no;			
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
			
		
--cleanup 
drop table if exists dev.am_opt_opioid_enrolled_2012;
drop table if exists dev.am_opt_opioid_enrolled_from_2013_to_2017_raw;
drop table if exists dev.am_opt_opioid_step_1;
drop table if exists dev.am_opt_opioid_step_2;
drop table if exists dev.am_opt_opioid_step_3;
drop table if exists dev.am_opt_opioid_step_4;
drop table if exists dev.am_opt_opioid_step_4a;
drop table if exists dev.am_opt_opioid_step_5;
drop table if exists dev.am_opt_opioid_step_5a;
drop table if exists dev.am_opt_opioid_step_6;
drop table if exists dev.am_opt_opioid_step_6a;
	
	
	
	
	
	
	
	
	
	
	
	