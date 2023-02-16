
--step 1,select uth_member_id who has consecutive 5 years enrollment from 2013-2017           
/*
drop table if exists dev.opt_opioid_tmp1_CE5;

select distinct uth_member_id,data_source,	rx_coverage,year ,total_enrolled_months
into dev.opt_opioid_tmp1_CE5
FROM data_warehouse.member_enrollment_yearly 
where  ("year" =2012 and rx_coverage = 1)                          
and uth_member_id in (select distinct uth_member_id 
					FROM data_warehouse.member_enrollment_yearly  where  ("year" =2013 and total_enrolled_months = 12 and rx_coverage = 1) )           
and uth_member_id in (select  distinct uth_member_id 
					FROM data_warehouse.member_enrollment_yearly  where  ("year" =2014 and total_enrolled_months = 12 and rx_coverage = 1) )           
and uth_member_id in (select  distinct uth_member_id 
					FROM data_warehouse.member_enrollment_yearly  where  ("year" =2015 and total_enrolled_months = 12 and rx_coverage = 1) )          
and uth_member_id in (select  distinct uth_member_id 
					FROM data_warehouse.member_enrollment_yearly  where  ("year" =2016 and total_enrolled_months = 12 and rx_coverage = 1) )           
and uth_member_id in (select  distinct uth_member_id 
					FROM data_warehouse.member_enrollment_yearly  where  ("year" =2017 and total_enrolled_months = 12  and rx_coverage = 1) )
and data_source ='optz' ;  
*/

drop table if exists dev.am_opt_opioid_members;

select distinct uth_member_id, data_source , rx_coverage , year, count(year) as total_enrolled_months
	into dev.am_opt_opioid_members
	from data_warehouse.member_enrollment_monthly mem 
	where rx_coverage = 1
		and data_source ='optz'
		and year = 2012
	group by uth_member_id , data_source, rx_coverage, year
	having count(year) = 12
	order by uth_member_id ;
	
insert into dev.am_opt_opioid_members( uth_member_id, data_source , rx_coverage , year, total_enrolled_months)	
	select distinct uth_member_id, data_source , rx_coverage , year, count(year) as total_enrolled_months		
		from data_warehouse.member_enrollment_monthly mem 
		where rx_coverage = 1
			and data_source ='optz'
			and year = 2013
		group by uth_member_id , data_source, rx_coverage, year
		having count(year) = 12
		order by uth_member_id ;
	
insert into dev.am_opt_opioid_members( uth_member_id, data_source , rx_coverage , year, total_enrolled_months)	
	select distinct uth_member_id, data_source , rx_coverage , year, count(year) as total_enrolled_months		
		from data_warehouse.member_enrollment_monthly mem 
		where rx_coverage = 1
			and data_source ='optz'
			and year = 2014
		group by uth_member_id , data_source, rx_coverage, year
		having count(year) = 12
		order by uth_member_id ;
	
insert into dev.am_opt_opioid_members( uth_member_id, data_source , rx_coverage , year, total_enrolled_months)	
	select distinct uth_member_id, data_source , rx_coverage , year, count(year) as total_enrolled_months		
		from data_warehouse.member_enrollment_monthly mem 
		where rx_coverage = 1
			and data_source ='optz'
			and year = 2015
		group by uth_member_id , data_source, rx_coverage, year
		having count(year) = 12
		order by uth_member_id 	;
		
insert into dev.am_opt_opioid_members( uth_member_id, data_source , rx_coverage , year, total_enrolled_months)	
	select distinct uth_member_id, data_source , rx_coverage , year, count(year) as total_enrolled_months		
		from data_warehouse.member_enrollment_monthly mem 
		where rx_coverage = 1
			and data_source ='optz'
			and year = 2016
		group by uth_member_id , data_source, rx_coverage, year
		having count(year) = 12
		order by uth_member_id 	;
			
insert into dev.am_opt_opioid_members( uth_member_id, data_source , rx_coverage , year, total_enrolled_months)	
	select distinct uth_member_id, data_source , rx_coverage , year, count(year) as total_enrolled_months		
		from data_warehouse.member_enrollment_monthly mem 
		where rx_coverage = 1
			and data_source ='optz'
			and year = 2017
		group by uth_member_id , data_source, rx_coverage, year
		having count(year) = 12
		order by uth_member_id 	;		

-------------------------------------------------------------------------------------------------------------	
drop table if exists dev.opt_opioid_tmp1_CE5;
	select distinct uth_member_id
		into dev.opt_opioid_tmp1_CE5
		from dev.am_opt_opioid_members		
		group by uth_member_id  	
		having count(year) = 6	;

--step 2,select pt who meet 5 years CE and meet opioid criteria by ndc with >=90 days supply
drop table if exists dev.opt_opioid_tmp2_ndc;
SELECT *
	into dev.opt_opioid_tmp2_ndc
	FROM data_warehouse.pharmacy_claims
	where "year" =2012 
		and ndc in (select ndc from dev.am_opioid_ndc)
		and days_supply >=90
		and uth_member_id in(select uth_member_id from dev.opt_opioid_tmp1_CE5);


  
--select what has opioid prescription in last 4 months of 2011(no need 90 days of supply condition)
drop table if exists dev.opt_opioid_tmp4_Opioid2011;
select *
	into dev.opt_opioid_tmp4_Opioid2011
	FROM data_warehouse.pharmacy_claims 
	where fill_date >='2011-09-01' and  fill_date <= '2011-12-31' 
	  and ndc in (select ndc from dev.am_opioid_ndc) 
	  and uth_member_id in(select uth_member_id from dev.opt_opioid_tmp1_CE5);

--(delete Opioid2011 from CE table)
delete
	from dev.opt_opioid_tmp2_ndc as ce
	where ce.uth_member_id in (select uth_member_id from dev.opt_opioid_tmp4_Opioid2011);

--step 5 who had cancer diagnosis by the cancer DX icd9 codes,  1/1/2012-12/31/2012
drop table if exists dev.opt_opioid_tmp5_cancer;
select uth_member_id, diag_cd,"date"
	into dev.opt_opioid_tmp5_cancer
	from data_warehouse.claim_diag
	where "year"=2012 and data_source='optz' and diag_cd in (select dx from dev.am_opioid_cancer);

    -- delete cancer patients
delete
	from dev.opt_opioid_tmp2_ndc as ce
	where ce.uth_member_id in (select uth_member_id from dev.opt_opioid_tmp5_cancer);

--step 6,who had a cognitive impairment diagnosis prior to 2012 , by DX icd9 codes,  before 12/31/2011
drop table if exists dev.opt_opioid_tmp6_ci;
select distinct uth_member_id, diag_cd,"date"
	into dev.opt_opioid_tmp6_ci
	from data_warehouse.claim_diag
	where data_source='optz' and "date" <= '2011-12-31' 
	and (diag_cd in ('29040','29041', '29042', '29043','29282')
	     or diag_cd like '331%' or diag_cd like '2941%' or diag_cd like '2942%' or diag_cd like '2948%' or diag_cd like '2949%');
	    
 --(delete cognitive impairment patients)
delete
	from dev.opt_opioid_tmp2_ndc as ce
	where ce.uth_member_id in (select uth_member_id from dev.opt_opioid_tmp6_ci);

 
--step 7 select continoius enrollment in 2012 after the date of first time prescription
drop table if exists dev.opt_opioid_tmp3_ce1;
SELECT  * 
	into dev.opt_opioid_tmp3_ce1
	FROM data_warehouse.pharmacy_claims
	where "year" =2012 
	and data_source = 'optz'
	and ndc in (select ndc from dev.am_opioid_ndc)
	and uth_member_id in (select uth_member_id from dev.opt_opioid_tmp2_ndc);
	 
--(select min prescription date for opoiod prescription)
drop table if exists dev.opt_opioid_tmp3_ce2;
select n1.uth_member_id,n1.fill_date
	into dev.opt_opioid_tmp3_ce2
	from dev.opt_opioid_tmp3_ce1 as n1
	where n1.fill_date = (select min(n2.fill_date)from dev.opt_opioid_tmp3_ce1 as n2 where n1.uth_member_id = n2.uth_member_id );

--select patients in 2012 who has continious enrollment after the first fill date.
--these patients also meet all the previous conditiions.
drop table if exists dev.opt_opioid_tmp3_ce3;
	select distinct y.uth_member_id,y.data_source,	y.rx_coverage, y.year ,total_enrolled_months,
	         (12-extract(month from m.fill_date))as diffdate,m.fill_date, y.bus_cd 
	into dev.opt_opioid_tmp3_ce3
	from data_warehouse.member_enrollment_yearly y
	join dev.opt_opioid_tmp3_ce2 m on y.uth_member_id =m.uth_member_id 
	where y.data_source='optz' and y.year=2012 and y.rx_coverage=1 
		and y.total_enrolled_months >=(12-extract(month from m.fill_date));

--convert uth_member_id to member_id_src
drop table if exists dev.opt_opioid_mem;
	select y.uth_member_id, d.uth_member_id dimuthmember, y.data_source,y.bus_cd, d.member_id_src
	into dev.opt_opioid_mem
	from dev.opt_opioid_tmp3_ce3 y, data_warehouse.dim_uth_member_id d
	where y.uth_member_id =d.uth_member_id ;
 


--get MCR claim data from raw tables
--confinement
DROP TABLE if exists dev.opt_opioid_Conf_MA;
SELECT distinct * 
INTO dev.opt_opioid_Conf_MA
FROM optum_zip.confinement as p
WHERE cast (p.patid as text) IN (SELECT member_id_src FROM dev.opt_opioid_mem where bus_cd ='MCR')
and p.year>=2012;

--diagnostic
DROP TABLE if exists dev.opt_opioid_diag_MA;
SELECT distinct * 
INTO dev.opt_opioid_diag_MA
FROM optum_zip.diagnostic as p
WHERE cast (p.patid as text) IN (SELECT member_id_src FROM dev.opt_opioid_mem where bus_cd ='MCR')
and p.year>=2012;

-- medical
drop table if exists  dev.opt_opioid_medical_MA;
SELECT distinct * 
INTO dev.opt_opioid_medical_MA
FROM optum_zip.medical as p
WHERE cast (p.patid as text) IN (SELECT member_id_src FROM dev.opt_opioid_mem where bus_cd ='MCR')
and p.year>=2012;

--procedure
drop table if exists  dev.opt_opioid_procedure_MA;
SELECT distinct * 
INTO dev.opt_opioid_procedure_MA
FROM optum_zip."procedure" as p
WHERE cast (p.patid as text) IN (SELECT member_id_src FROM dev.opt_opioid_mem where bus_cd ='MCR')
and p.year>=2012;

-- RX
drop table if exists  dev.opt_opioid_rx_MA;
SELECT distinct * 
INTO dev.opt_opioid_rx_MA
FROM optum_zip.rx as p
WHERE cast (p.patid as text) IN (SELECT member_id_src FROM dev.opt_opioid_mem where bus_cd ='MCR')
and p.year>=2012;


--get COM claim data from raw tables

--confinement
DROP TABLE if exists dev.opt_opioid_Conf_COM;
SELECT distinct * 
INTO dev.opt_opioid_Conf_COM
FROM optum_zip.confinement as p
WHERE cast (p.patid as text) IN (SELECT member_id_src FROM dev.opt_opioid_mem where bus_cd ='COM')
and p.year>=2012;

--diagnostic
DROP TABLE if exists dev.opt_opioid_diag_COM;
SELECT distinct * 
INTO dev.opt_opioid_diag_COM
FROM optum_zip.diagnostic as p
WHERE cast (p.patid as text) IN (SELECT member_id_src FROM dev.opt_opioid_mem where bus_cd ='COM')
and p.year>=2012;

-- medical
drop table if exists  dev.opt_opioid_medical_COM;
SELECT distinct * 
INTO dev.opt_opioid_medical_COM
FROM optum_zip.medical as p
WHERE cast (p.patid as text) IN (SELECT member_id_src FROM dev.opt_opioid_mem where bus_cd ='COM')
and p.year>=2012;

--procedure
drop table if exists  dev.opt_opioid_procedure_COM;
SELECT distinct * 
INTO dev.opt_opioid_procedure_COM
FROM optum_zip."procedure" as p
WHERE cast (p.patid as text) IN (SELECT member_id_src FROM dev.opt_opioid_mem where bus_cd ='COM')
and p.year>=2012;

-- RX
drop table if exists  dev.opt_opioid_rx_COM;
SELECT distinct * 
INTO dev.opt_opioid_rx_COM
FROM optum_zip.rx as p
WHERE cast (p.patid as text) IN (SELECT member_id_src FROM dev.opt_opioid_mem where bus_cd ='COM')
and p.year>=2012;


/*
select count(*) from dev.opt_opioid_mem where bus_cd = 'MCR'
select count(*) from dev.opt_opioid_mem where bus_cd = 'COM'
 
drop table if exists dev.opt_opioid_tmp1_CE5;
drop table if exists dev.opt_opioid_tmp2_ndc;
drop table if exists dev.opt_opioid_tmp4_Opioid2011;
drop table if exists dev.opt_opioid_tmp5_cancer;
drop table if exists dev.opt_opioid_tmp6_ci;
drop table if exists dev.opt_opioid_tmp3_ce1;
drop table if exists dev.opt_opioid_tmp3_ce2;
drop table if exists dev.opt_opioid_tmp3_ce3;
drop table if exists dev.opt_opioid_mem;
*/