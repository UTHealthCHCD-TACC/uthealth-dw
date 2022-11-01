drop table if exists dw_staging.member_enrollment_monthly;

create table dw_staging.member_enrollment_monthly  
(like data_warehouse.member_enrollment_monthly including defaults) 
with (
		appendonly=true, 
		orientation=row, 
		compresstype=zlib, 
		compresslevel=5 
	 )
distributed by (uth_member_id)
partition by list(data_source)
 (partition optz values ('optz'),
  partition optd values ('optd'),
  partition truv values ('truv'),
  partition mdcd values ('mdcd'),
  partition mcrt values ('mcrt'),
  partition mcrn values ('mcrn')
 )
;

alter table  dw_staging.member_enrollment_monthly add column row_id bigserial;
alter sequence dw_staging.member_enrollment_monthly_row_id_seq cache 200;

drop table if exists dw_staging.medicaid_enroll_etl;

CREATE TABLE dw_staging.medicaid_enroll_etl (
	client_nbr text NULL,
	year int2 null,
	month_year_id int4 NULL,
	elig_date_month date NULL,
	sex text NULL,
	zip text NULL,
	zip3 text NULL,
	yr_end_date date NULL,
	dob date NULL,
	contract_id text NULL,
	year_fy int2 NULL,
	race text NULL,
	smib text null,
	me_code text null,
	table_id_src unknown NULL,
	plan_type text NULL,
	state text null
)
DISTRIBUTED BY (client_nbr);

insert into dw_staging.medicaid_enroll_etl 
select trim(a.client_nbr) as client_nbr ,
       substring(trim(elig_date),1,4)::int as year, 
       trim(elig_date)::int as month_year_id,
       (substring(trim(elig_date),5,6) || '-01-' || substring(trim(elig_date),1,4))::Date as elig_date_month,
       trim(a.sex) as sex, 
       trim(a.zip) as zip, 
       substring(a.zip,1,3) as zip3, 
       ('12-31-' || substring(elig_date,1,4))::Date as yr_end_date,
       trim(a.dob)::date as dob, 
       trim(a.contract_id) as contract_id, 
       year_fy, 
       trim(a.race) as race,
       trim(a.smib) as smib ,
       trim(a.me_code) as me_code,
	   'enrl' as table_id_src,
	   null as plan_type,
	   null as state
  from medicaid.enrl  a 
;

analyze dw_staging.medicaid_enroll_etl;

---
insert into dw_staging.medicaid_enroll_etl 
select trim(a.client_nbr) as client_nbr ,
       substring(trim(elig_month),1,4)::int as year, 
       trim(elig_month)::int as month_year_id,
       (substring(trim(elig_month),5,6) || '-01-' || substring(trim(elig_month),1,4))::Date as elig_month_month,
       trim(a.gender_cd) as sex, 
       substring(trim(a.mailing_zip),1,5) as zip, 
       substring(a.mailing_zip,1,3) as zip3, 
       ('12-31-' || substring(elig_month,1,4))::Date as yr_end_date,
       to_date( substring(date_of_birth,6,4) || substring(date_of_birth,3,3) || substring(date_of_birth,1,2) ,'YYYYMonDD') as dob, 
       null as contract_id, 
       year_fy, 
       trim(a.ethnicity) as race,
       null as smib,
       null as me_code,
	   'chip_uth' as table_id_src,
	   'CHIP' as plan_type,
	   null as state
  from medicaid.chip_uth a 
;

analyze dw_staging.medicaid_enroll_etl;

--- htw

insert into dw_staging.medicaid_enroll_etl 
select trim(a.client_nbr) as client_nbr ,
       substring(trim(elig_date),1,4)::int as year, 
       trim(elig_date)::int as month_year_id,
       (substring(trim(elig_date),5,6) || '-01-' || substring(trim(elig_date),1,4))::Date as elig_date_month,
       trim(a.sex) as sex, 
       trim(a.zip) as zip, 
       substring(a.zip,1,3) as zip3, 
       ('12-31-' || substring(elig_date,1,4))::Date as yr_end_date,
       trim(a.dob)::date as dob, 
       trim(a.contract_id) as contract_id, 
       case 
       		when substring(elig_date,5,2)::int >= 9 
       		then substring(elig_date,1,4)::int + 1
       		else substring(elig_date,1,4)::int
       	end as fiscal_year, 
       trim(a.race) as race,
       trim(a.smib) as smib ,
       trim(a.me_code) as me_code,
	   'htw_enrl' as table_id_src,
	   null as plan_type,
	   null as state
  from medicaid.htw_enrl a 
;

analyze dw_staging.medicaid_enroll_etl;
---------------update values------------------

update dw_staging.medicaid_enroll_etl a 
   set plan_type =  c.mco_program_nm
  from reference_tables.medicaid_lu_contract c 
 where c.plan_cd = a.contract_id
   and a.plan_type is null;  
  
update dw_staging.medicaid_enroll_etl a 
   set state =  z.state
  from reference_tables.ref_zip_code z 
 where a.zip = z.zip ;

update dw_staging.medicaid_enroll_etl a 
   set race = r.race_cd 
  from reference_tables.ref_race r 
 where r.race_cd_src = a.race
   and r.data_source = 'mdcd';
  
 update dw_staging.medicaid_enroll_etl a 
     set sex = 'U' 
   where sex not in ('F','M','U');

vacuum analyze dw_staging.medicaid_enroll_etl;




------------------------ get most common birthday ----------------------

 drop table if exists dev.birth_dupes1 ;
 
  select client_nbr, dob, count(*) as d_count, max(month_year_id) as recent 
    into dev.birth_dupes1 
    from dw_staging.medicaid_enroll_etl 
   group by client_nbr, dob ;
  
  
  drop table if exists dev.birth_dupes2;
    
   select *,
  	     row_number() over (partition by client_nbr order by d_count desc, recent desc) as dob_row
    into dev.birth_dupes2
  	from dev.birth_dupes1;
  
  drop table if exists dev.birth_dupes3;
  
  select client_nbr, dob 
    into dev.birth_dupes3
  	from dev.birth_dupes2
   where dob_row = 1;
  
  update dw_staging.medicaid_enroll_etl a
    set dob = b.dob 
   from dev.birth_dupes3 b 
  where a.client_nbr = b.client_nbr ;
  
 vacuum analyze dw_staging.medicaid_enroll_etl;

 drop table if exists dev.birth_dupes3;
 drop table if exists dev.birth_dupes2;
 drop table if exists dev.birth_dupes1;

------------------------------------------------------------


delete from dw_staging.medicaid_enroll_etl a 
 where plan_type <> 'CHIP'
 and exists (
 	select 1 
 	from dw_staging.medicaid_enroll_etl b 
 	where a.client_nbr = b.client_nbr 
 	  and a.month_year_id = b.month_year_id 
 	and b.plan_type = 'CHIP'
 );


delete from dw_staging.medicaid_enroll_etl a 
 where plan_type = 'STAR'
 and exists (
 	select 1 
 	from dw_staging.medicaid_enroll_etl b 
 	where a.client_nbr = b.client_nbr 
 	  and a.month_year_id = b.month_year_id 
 	and b.plan_type in ('STAR Health','STAR Kids')
 );

delete from dw_staging.medicaid_enroll_etl a 
 where plan_type = 'STAR Health'
 and exists (
 	select 1 
 	from dw_staging.medicaid_enroll_etl b 
 	where a.client_nbr = b.client_nbr 
 	  and a.month_year_id = b.month_year_id 
 	and b.plan_type in ('STAR Kids','STAR+PLUS')
 );


delete from dw_staging.medicaid_enroll_etl a 
 where exists (
 	select 1 
 	from dw_staging.medicaid_enroll_etl b 
 	where a.client_nbr = b.client_nbr 
 	  and a.month_year_id = b.month_year_id 
 	  and a.plan_type <> b.plan_type 
 	  and b.plan_type like '%STAR%'
 );

delete from dw_staging.medicaid_enroll_etl a 
 where plan_type is null and exists (
 	select 1 
 	from dw_staging.medicaid_enroll_etl b 
 	where a.client_nbr = b.client_nbr 
 	  and a.month_year_id = b.month_year_id 
 	  and b.plan_type like '%STAR%'
 );

vacuum analyze dw_staging.medicaid_enroll_etl;

---------- -------------------------------------
insert into dw_staging.member_enrollment_monthly (
	data_source, 
	year, 
	month_year_id, 
	uth_member_id,
	gender_cd, 
	state, 
	zip5, 
	zip3,
	age_derived, 
	dob_derived, 
	death_date,
	plan_type, 
	bus_cd, 
	rx_coverage, 
	fiscal_year, 
	race_cd,
	dual,
	htw,
	age_months,
	table_id_src,
	member_id_src,
	load_date 
	)		
	select 
	'mdcd',
	year, 
	month_year_id,
	b.uth_member_id,
	a.sex,
	a.state,
	a.zip,
	a.zip3,
	extract( years from age(a.yr_end_date, dob)),
	dob,
	null as death_date,
	a.plan_type,
	null as bus_cd, 
	1 as rx_coverage,
	year_fy,
	a.race,
	case 
       	when a.smib = '1' then 1 else 0
       end as dual,
    case 
       	when a.me_code = 'W' then 1 else 0
       end as htw,
    ((extract(months from age(elig_date_month, dob))) + ((extract(years from age(elig_date_month, dob))) * 12)) as months_old,
    a.table_id_src,
    a.client_nbr as member_id_src,
    current_date as load_date
from dw_staging.medicaid_enroll_etl  a 
  join data_warehouse.dim_uth_member_id b  
     on b.data_source = 'mdcd'
    and b.member_id_src = a.client_nbr 	
	;

analyze dw_staging.member_enrollment_monthly;


---**script to build consecutive enrolled months	
drop table if exists dev.temp_consec_enrollment;

create table dev.temp_consec_enrollment 
with (appendonly=true, orientation=column) as 
select row_id::bigint as row_id
      ,row_number() over(partition by uth_member_id, my_grp order by  month_year_id) as in_streak
from ( 
	   select a.row_id
	         ,a.month_year_id
	         ,a.uth_member_id
	         ,b.my_row_counter - row_number() over(partition by a.uth_member_id order by a.month_year_id) as my_grp
	   from dw_staging.member_enrollment_monthly 	 a 
	     join reference_tables.ref_month_year b 
	       on a.month_year_id = b.month_year_id 	   		    
	 ) inr    
distributed by (row_id);

analyze dev.temp_consec_enrollment;

--update consec enrolled months  9m 
update dw_staging.member_enrollment_monthly a 
   set consecutive_enrolled_months = b.in_streak 
  from dev.temp_consec_enrollment b 
 where a.row_id = b.row_id
;

--**cleanup
drop table if exists dev.temp_consec_enrollment;

---/drop sequence, rebuild table distributed on uth_member_id 
alter table dw_staging.member_enrollment_monthly drop column row_id;
vacuum full analyze dw_staging.member_enrollment_monthly;
alter table dw_staging.member_enrollment_monthly owner to uthealth_dev;
grant select on dw_staging.member_enrollment_monthly to uthealth_analyst;
