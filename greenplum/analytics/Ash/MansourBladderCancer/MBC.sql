drop table if exists dev.am_mbc_members_diag;

select distinct uth_member_id , min(from_date_of_service) dt, year , 'diag' as type
	into dev.am_mbc_members_diag
	from data_warehouse.claim_diag d 
	where data_source = 'optd'		
		and (
				(
					"year" in (2014, 2015)
					and icd_type = '9'
					and diag_cd in ('1889', 														     -- dx codes
									'99832', '99749', '9975', '56969', '3051', 'V1582', '9900', '9904') -- complication dx codes	
				)
				or							 				  	
				(
					"year" in (2015, 2016, 2017)
					and icd_type <> '9'
					and diag_cd in ('C67', 'C670', 'C671', 'C672', 'C673', 'C674', 'C675', 'C676', 'C677', 'C678', 'C679', -- dx codes
									'T813', 'K913', 'N990', 'N9952', 'N9953', 'F1720', 'Z720') 							   -- complication dx codes	
				)
				
			)
	group by d.uth_member_id, year  
	order by uth_member_id;

--select * from dev.am_mbc_members_diag order by uth_member_id
---------------------------------------------------------------------------------------------------------
drop table if exists dev.am_mbc_members_cpt_hcpcs;	

select distinct c.uth_member_id , min(from_date_of_service) dt, c.year , 'cpt_hcpcs' as type 
	into dev.am_mbc_members_cpt_hcpcs
	from data_warehouse.claim_detail c, dev.am_mbc_members_diag d 
	where c.uth_member_id = d.uth_member_id
		and c."year"  in (2014, 2015, 2016, 2017)
		and c.data_source = 'optd'		
		and c.cpt_hcpcs in ('51596', '51580', '51590', '51575', '51596', '51585', '51595', '55866', 
							'99201', '99202', '99203', '99204', '99205', '99211', '99212', '99213',
							'99214', '99215', '99221', '99222', '99223', '99224', '99225', '99226',
							'99231', '99232', '99233', '99234', '99235', '99236', '99238', '99239',
							'P9010', 'P9051', 'P9054', 'P9056', 'P9056', 'P9057', 'P9058') 
	group by c.uth_member_id, c.year 
	order by c.uth_member_id;					
							
--select * from dev.am_mbc_members_cpt_hcpcs order by uth_member_id						
---------------------------------------------------------------------------------------------------------							
drop table if exists dev.am_mbc_members_procs;									
							
select distinct p.uth_member_id , min(from_date_of_service) dt, p.year , 'procs' as type
	into dev.am_mbc_members_procs
	from data_warehouse.claim_icd_proc p, dev.am_mbc_members_diag d  
	where  p.uth_member_id = d.uth_member_id
		and data_source = 'optd'		
		and (
				(
					p."year" in (2014, 2015)
					and icd_type = '9'
					and proc_cd in ('5771', '5779', '9900', '9904') 	
				)
				or							 				  	
				(
					p."year" in (2015, 2016, 2017)
					and icd_type <> '9'
					and proc_cd in ('0TTB0ZZ', '0TTB4ZZ', '0TTB7ZZ', '0TTB8ZZ', '30233H0', '30233N0')
				)				
			)
	group by p.uth_member_id, p.year  
	order by p.uth_member_id;

--select * from dev.am_mbc_members_procs order by uth_member_id	
---------------------------------------------------------------------------------------------------------
--get all rows from cpt_hcpcs
drop table if exists dev.am_mbc_dx;
select distinct d.uth_member_id , min(d.dt ) as first_date_of_dx, 
		'N' as dx_2014, 'N' as dx_2015, 'N' as dx_2016, 'N' as dx_2017,
		max(d.dt ) as first_date_of_proc, 0 as continuous_enrollment_months
	into dev.am_mbc_dx
	from dev.am_mbc_members_diag d, dev.am_mbc_members_cpt_hcpcs c
	where d.uth_member_id = c.uth_member_id 
	group by d.uth_member_id 
	order by d.uth_member_id; 

--get rows from procs
insert into dev.am_mbc_dx
select distinct d.uth_member_id , min(d.dt ) as first_date_of_dx, 
		'N' as dx_2014, 'N' as dx_2015, 'N' as dx_2016, 'N' as dx_2017,
		max(d.dt ) as first_date_of_proc, 0 as continuous_enrollment_months
	from dev.am_mbc_members_diag d, dev.am_mbc_members_procs p
	where d.uth_member_id = p.uth_member_id 
		and not exists (select uth_member_id from dev.am_mbc_dx x where x.uth_member_id = d.uth_member_id)
	group by d.uth_member_id 
	order by d.uth_member_id; 
---------------------------------------------------------------------------------------------------------
--set dx_2014
drop table if exists dev.am_mbc_year; 

select distinct d.uth_member_id
	into dev.am_mbc_year
	from dev.am_mbc_members_diag d, dev.am_mbc_members_cpt_hcpcs c
		where d.uth_member_id = c.uth_member_id 	 
			and date_part('year', d.dt) = 2014;	
		
insert into dev.am_mbc_year
select distinct d.uth_member_id   
	from dev.am_mbc_members_diag d, dev.am_mbc_members_procs p
	where d.uth_member_id = p.uth_member_id 		
		and date_part('year', d.dt) = 2014 	
		and not exists (select uth_member_id from dev.am_mbc_year y where y.uth_member_id = d.uth_member_id);		

update dev.am_mbc_dx x
		set dx_2014 = 'Y'
	from dev.am_mbc_year y
	where x.uth_member_id = y.uth_member_id ;
---------------------------------------------------------------------------------------------------------
--set dx_2015
drop table if exists dev.am_mbc_year; 

select distinct d.uth_member_id
	into dev.am_mbc_year
	from dev.am_mbc_members_diag d, dev.am_mbc_members_cpt_hcpcs c
		where d.uth_member_id = c.uth_member_id 	 
			and date_part('year', d.dt) = 2015;	
		
insert into dev.am_mbc_year
select distinct d.uth_member_id   
	from dev.am_mbc_members_diag d, dev.am_mbc_members_procs p
	where d.uth_member_id = p.uth_member_id 		
		and date_part('year', d.dt) = 2015 	
		and not exists (select uth_member_id from dev.am_mbc_year y where y.uth_member_id = d.uth_member_id);		

update dev.am_mbc_dx x
		set dx_2015 = 'Y'
	from dev.am_mbc_year y
	where x.uth_member_id = y.uth_member_id ;
---------------------------------------------------------------------------------------------------------
--set dx_2016
drop table if exists dev.am_mbc_year; 

select distinct d.uth_member_id
	into dev.am_mbc_year
	from dev.am_mbc_members_diag d, dev.am_mbc_members_cpt_hcpcs c
		where d.uth_member_id = c.uth_member_id 	 
			and date_part('year', d.dt) = 2016;	
		
insert into dev.am_mbc_year
select distinct d.uth_member_id   
	from dev.am_mbc_members_diag d, dev.am_mbc_members_procs p
	where d.uth_member_id = p.uth_member_id 		
		and date_part('year', d.dt) = 2016 	
		and not exists (select uth_member_id from dev.am_mbc_year y where y.uth_member_id = d.uth_member_id);		

update dev.am_mbc_dx x
		set dx_2016 = 'Y'
	from dev.am_mbc_year y
	where x.uth_member_id = y.uth_member_id ;
---------------------------------------------------------------------------------------------------------
--set dx_2017
drop table if exists dev.am_mbc_year; 

select distinct d.uth_member_id
	into dev.am_mbc_year
	from dev.am_mbc_members_diag d, dev.am_mbc_members_cpt_hcpcs c
		where d.uth_member_id = c.uth_member_id 	 
			and date_part('year', d.dt) = 2017;	
		
insert into dev.am_mbc_year
select distinct d.uth_member_id   
	from dev.am_mbc_members_diag d, dev.am_mbc_members_procs p
	where d.uth_member_id = p.uth_member_id 		
		and date_part('year', d.dt) = 2017 	
		and not exists (select uth_member_id from dev.am_mbc_year y where y.uth_member_id = d.uth_member_id);		

update dev.am_mbc_dx x
		set dx_2017 = 'Y'
	from dev.am_mbc_year y
	where x.uth_member_id = y.uth_member_id ;

--select * from dev.am_mbc_dx order by uth_member_id
---------------------------------------------------------------------------------------------------------
--reset proc date
update dev.am_mbc_dx x
	set first_date_of_proc = null;

--get first date of proc from cpt_hcpcs
drop table if exists dev.am_mbc_proc_dt; 
select distinct c.uth_member_id , min(c.dt) as first_date_of_proc
	into dev.am_mbc_proc_dt
	from dev.am_mbc_members_cpt_hcpcs c
	group by c.uth_member_id ;

--get first date of proc from procs
insert into dev.am_mbc_proc_dt
select distinct p.uth_member_id , min(p.dt) as first_date_of_proc
	from dev.am_mbc_members_procs p
	group by p.uth_member_id ;


drop table if exists dev.am_mbc_proc_dt_final;
select distinct uth_member_id, min(first_date_of_proc) as first_date_of_proc
	into dev.am_mbc_proc_dt_final
	from dev.am_mbc_proc_dt
	group by uth_member_id;
	
update dev.am_mbc_dx x
	set first_date_of_proc = p.first_date_of_proc
	from dev.am_mbc_proc_dt_final p
	where p.uth_member_id = x.uth_member_id ;
	
--select * from dev.am_mbc_dx order by uth_member_id	
---------------------------------------------------------------------------------------------------------
--calculate consecutive enrollment months 
--if proc month june and year is 2016 and person enrolled for jun, jul, aug then consecutive months is 3
--even if the person is enrolled in 2017 again for the whole year 

drop table if exists dev.am_mbc_enrollment_from_proc_1;
with row_build_cte as ( 
	select row_identifier 
	      ,row_number() over(partition by uth_member_id, my_grp order by  month_year_id) as in_streak
	from ( 
		   select a.row_identifier
		         ,a.month_year_id
		         ,a.uth_member_id
		         ,b.my_row_counter - row_number() over(partition by a.uth_member_id order by a.month_year_id) as my_grp
		   from data_warehouse.member_enrollment_monthly a 
   	       join reference_tables.ref_month_year b on a.month_year_id = b.month_year_id 	
   	       join dev.am_mbc_dx x on x.uth_member_id = a.uth_member_id 
   	       where to_date(a.month_year_id::text, 'YYYYMMDD') between x.first_date_of_proc and to_date('20191231', 'YYYYMMDD') 	
		 ) sub    
) 
select distinct uth_member_id , month_year_id , consecutive_enrolled_months 
	into dev.am_mbc_enrollment_from_proc_1
	from data_warehouse.member_enrollment_monthly c , row_build_cte d
	where c.row_identifier = d.row_identifier;


drop table if exists dev.am_mbc_enrollment_from_proc;
select distinct uth_member_id, max(consecutive_enrolled_months) - min (consecutive_enrolled_months) + 1 as consecutive_enrolled_months
	into dev.am_mbc_enrollment_from_proc
	from dev.am_mbc_enrollment_from_proc_1	
	group by uth_member_id
	order by uth_member_id;
---------------------------------------------------------------------------------------------------------
--update consecutive enrollment months	 
--select * from dev.am_mbc_enrollment_from_proc amefp 

update dev.am_mbc_dx x
		set continuous_enrollment_months = e.consecutive_enrolled_months 
	from dev.am_mbc_enrollment_from_proc e
	where e.uth_member_id = x.uth_member_id ;

--select * from dev.am_mbc_dx order by uth_member_id
---------------------------------------------------------------------------------------------------------
--get age and other ennrollment data based on first proc year
drop table if exists dev.am_mbc_enrollment;

select distinct y.uth_member_id , year, age_derived , race_cd , gender_cd , state , bus_cd , y.total_enrolled_months 
	into dev.am_mbc_enrollment
	from data_warehouse.member_enrollment_yearly y, dev.am_mbc_dx x
	where y.uth_member_id = x.uth_member_id 
		 and y.year = date_part('year',  x.first_date_of_proc) ;

--select * from dev.am_mbc_enrollment
---------------------------------------------------------------------------------------------------------	
drop table if exists dev.am_mbc_confinement; 	
	
select distinct x.uth_member_id , m.member_id_src , c.conf_id , c.admit_date , c.disch_date , c.dstatus 
	into dev.am_mbc_confinement
	from dev.am_mbc_dx x, data_warehouse.dim_uth_member_id m, optum_dod.confinement c 
	where x.uth_member_id = m.uth_member_id 
		and m.member_id_src = c.patid::text 
		and c.year between 2014 and 2019;
 
--select * from dev.am_mbc_confinement;
---------------------------------------------------------------------------------------------------------
--cleanup 
drop table if exists dev.am_mbc_members_cpt_hcpcs;
drop table if exists dev.am_mbc_members_diag;
drop table if exists dev.am_mbc_members_procs;
drop table if exists dev.am_mbc_year;
drop table if exists dev.am_mbc_proc_dt;
drop table if exists dev.am_mbc_proc_dt_final;
drop table if exists dev.am_mbc_enrollment_from_proc_1	;











