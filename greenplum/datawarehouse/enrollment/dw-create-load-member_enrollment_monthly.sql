/* ******************************************************************************************************
 *  The member_enrollment_monthly table creates one record for each month/year that a member was enrolled in coverage
 *  
 *  Run the relevant code section for the dataset in (---------------- data loads --------------------)
 * 
 *  !!!!!!!!!  data_warehouse.dim_member_id_src table must be populated first !!!!!!!!!   
 *   	             Use dw-create-load-dim_member_id_src.sql in Git    
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
 * 
*/


----  // BEGIN SCRIPT 

---create working table in dw_staging 
drop table if exists dw_staging.member_enrollment_monthly ;

create table dw_staging.member_enrollment_monthly  (
	data_source char(4),
	year int2, 
	uth_member_id bigint,
	month_year_id int4, 
	consecutive_enrolled_months int4, 
	gender_cd char(1), 
	race_cd char(1),
	age_derived int4, 
	dob_derived date, 
	state text, 
	zip5 char(5), 
	zip3 char(3), 
	death_date date, 
	plan_type text, 
	bus_cd char(4), 
	employee_status text, 
	claim_created_flag boolean default false,
	rx_coverage int2, 
	fiscal_year int2,
	row_id bigserial
) distributed by (uth_member_id);



---
                                                                           
alter sequence dw_staging.member_enrollment_monthly_row_id_seq cache 200;

vacuum analyze dw_staging.member_enrollment_monthly;



--(---------------- data loads --------------------)


-- ***** Optum DOD ***** --------------------------------------------------------------------------------------
delete from dw_staging.member_enrollment_monthly where data_source = 'optd';

insert into dw_staging.member_enrollment_monthly (
	data_source, year, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd, rx_coverage, fiscal_year, race_cd       
	)		
select 'optd', b.year_int, b.month_year_id, a.uth_member_id,
       c.gender_cd, m.state, null, null, 
       b.year_int - yrdob, case when yrdob = 0 then null else (yrdob::varchar || '-12-31')::date end as birth_dt, 
       case when death_ym is null then null else death_ym end as death_dt,  
       d.plan_type, bus, 1 as rx, b.year_int, r.race_cd 
from optum_dod.mbr_enroll_r m
  join data_warehouse.dim_uth_member_id a
    on a.member_id_src = m.patid::text
   and a.data_source = 'optd'
  left outer join optum_dod.mbrwdeath dth 
    on dth.patid = m.patid 
  join reference_tables.ref_month_year b
    on b.start_of_month between date_trunc('month', m.eligeff) and case when dth.death_ym is not null then dth.death_ym else m.eligend end   ---wcc002
  left outer join reference_tables.ref_gender c
    on c.data_source = 'opt'
   and c.gender_cd_src = m.gdr_cd 
  left outer join reference_tables.ref_plan_type d
    on d.data_source = 'opt'
   and d.plan_type_src = m.product
  left outer join reference_tables.ref_race r 
    on r.race_cd_src = m.race 
   and r.data_source = 'optd'
;
---------------------------------------------------------------------------------------------------


-- ***** Optum ZIP ***** --------------------------------------------------------------------------------------
delete from dw_staging.member_enrollment_monthly where data_source = 'optz';

insert into dw_staging.member_enrollment_monthly  (
	data_source, year, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd, rx_coverage, fiscal_year, race_cd         
	)	
select 
	   'optz',b.year_int, b.month_year_id, a.uth_member_id,
       c.gender_cd, e.state, substring(zipcode_5,1,5), substring(zipcode_5,1,3),
       b.year_int - yrdob, case when yrdob = 0 then null else (yrdob::varchar || '-12-31')::date end as birth_dt, null, 
       d.plan_type, bus, 1 as rx, b.year_int, r.race_cd  
from optum_zip.mbr_enroll m
  join data_warehouse.dim_uth_member_id a
    on a.member_id_src = m.patid::text
   and a.data_source = 'optz'
  join reference_tables.ref_month_year b
    on b.start_of_month between date_trunc('month', m.eligeff) and m.eligend
  left outer join reference_tables.ref_gender c
    on c.data_source = 'opt'
   and c.gender_cd_src = m.gdr_cd
  left outer join reference_tables.ref_plan_type d
    on d.data_source = 'opt'
   and d.plan_type_src = m.product
  left outer join reference_tables.ref_zip_crosswalk e 
   on e.zip = substring(zipcode_5,1,5) 
    left outer join reference_tables.ref_race r --wc003 
    on r.race_cd_src = null
   and r.data_source = 'optz' 
; 
---------------------------------------------------------------------------------------------------


------ **** Truven *******
delete from dw_staging.member_enrollment_monthly where data_source = 'truv';

create table dev.truven_uth_mem
with(appendonly=true,orientation=column)
as select *
from data_warehouse.dim_uth_member_id where data_source = 'truv'
distributed by(member_id_src);

vacuum analyze dev.truven_uth_mem;


create table truven.ccaet_temp 
with (appendonly=true, orientation=column) as 
select * 
from truven.ccaet
distributed by (enrolid)
;

vacuum analyze truven.ccaet_temp;


select count(*), year 
from optum_zip.medical 
group by year 
order by year 
;


-- Truven Commercial ----------------------------------------------------------------------------
-- 9/2/21 runtime 58min
insert into dw_staging.member_enrollment_monthly  (
	data_source, year, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd, employee_status, rx_coverage, fiscal_year , race_cd        
	)		
select 
	   'truv', b.year_int, b.month_year_id, a.uth_member_id, 
       c.gender_cd, case when length(s.abbr) > 2 then '' else s.abbr end, null, trunc(m.empzip,0)::text,
       b.year_int - dobyr, (trunc(dobyr,0)::varchar || '-12-31')::date, null, 
       d.plan_type, 'COM', eestatu, m.rx, m.year , '0' as race
from truven.ccaet_temp m
  join dev.truven_uth_mem a --join data_warehouse.dim_uth_member_id a
    on a.member_id_src = m.enrolid::text
   and a.data_source = 'truv'
  join reference_tables.ref_truven_state_codes s 
    on m.egeoloc=s.truven_code
  join reference_tables.ref_month_year b 
    on b.start_of_month between date_trunc('month', m.dtstart) and m.dtend
  left outer join reference_tables.ref_gender c
    on c.data_source = 'trv'
   and c.gender_cd_src = m.sex::text
  left outer join reference_tables.ref_plan_type d
    on d.data_source = 'trv'
  and d.plan_type_src::int = m.plantyp  
;
---------------------------------------------------------------------------------------------------





-- Truven Medicare Advantage ----------------------------------------------------------------------
insert into dw_staging.member_enrollment_monthly  (
	data_source, year, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd, employee_status, rx_coverage , fiscal_year, race_cd     
	)		
select 
       'truv', b.year_int,b.month_year_id, a.uth_member_id,
       c.gender_cd, case when length(s.abbr) > 2 then '' else s.abbr end, null, trunc(m.empzip,0)::text,
       b.year_int - dobyr, (trunc(dobyr,0)::varchar || '-12-31')::date, null,
       d.plan_type, 'MCR', eestatu, m.rx, m.year, '0' as race 
from truven.mdcrt m
  join dev.truven_uth_mem a  --join data_warehouse.dim_uth_member_id a
    on a.member_id_src = m.enrolid::text
   and a.data_source = 'truv'
  join reference_tables.ref_truven_state_codes s 
	on m.egeoloc=s.truven_code
  join reference_tables.ref_month_year b
    on b.start_of_month between date_trunc('month', m.dtstart) and m.dtend
  left outer join reference_tables.ref_gender c
    on c.data_source = 'trv'
   and c.gender_cd_src = m.sex::text
  left outer join reference_tables.ref_plan_type d
    on d.data_source = 'trv'
  and d.plan_type_src::int = m.plantyp
;
---------------------------------------------------------------------------------------------------

drop table dev.truven_uth_mem;


vacuum analyze dw_staging.member_enrollment_monthly;

select count(*), data_source, year 
from dw_staging.member_enrollment_monthly mem 
group by data_source, year  
order by data_source, year 




----- End Truven ******


/*
create table reference_tables.ref_medicare_entlmt_buyin (buyin_cd char(1), plan_type text);

insert into reference_tables.ref_medicare_entlmt_buyin values 
			('0',null),('1','A'),('2','B'),('3','AB'),
			('A','A'),('B','B'),('C','AB');

create table reference_tables.ref_medicare_ptd_cntrct (ptd_first_char char(1), ptd_coverage int2);

insert into reference_tables.ref_medicare_ptd_cntrct values 
            ('E',1),('H',1),('R',1),('S',1),
            ('X',1),('N',0),('0',0),(null,0);



select distinct substring(mas.ptd_cntrct_id_01,1,1) from medicare_texas.mbsf_abcd_summary mas 
*/



-- Medicare  Texas--------------------------------------------------------------------------------------
delete from dw_staging.member_enrollment_monthly where data_source = 'mcrt';

insert into dw_staging.member_enrollment_monthly (
	data_source, year, month_year_id, uth_member_id,
	gender_cd, state, zip5 , zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd, rx_coverage, fiscal_year , race_cd     
	)		
select 'mcrt',b.year_int, b.month_year_id, a.uth_member_id,
	   c.gender_cd,case when e.state_cd is null then 'XX' else e.state_cd end, m.zip_cd, substring(m.zip_cd,1,3),
	   bene_enrollmt_ref_yr::int - extract( year from bene_birth_dt::date),bene_birth_dt::date, bene_death_dt::date,
	   ent.plan_type, 'MDCR', ptd.ptd_coverage, m.year::int2, r.race_cd 
from medicare_texas.mbsf_abcd_summary m
  join data_warehouse.dim_uth_member_id a
    on a.member_id_src = m.bene_id::text
   and a.data_source = 'mcrt'
  left outer join reference_tables.ref_gender c
    on c.data_source = 'mcr'
   and c.gender_cd_src = m.sex_ident_cd
  left outer join reference_tables.ref_medicare_state_codes e 
     on e.medicare_state_cd = m.state_code   
  left outer join reference_tables.ref_race r 
     on r.race_cd_src = m.bene_race_cd 
    and r.data_source = 'mcrt'
  join reference_tables.ref_month_year b
    on b.year_int = bene_enrollmt_ref_yr::int
   and 
   (	month_int = case when m.mdcr_status_code_01 in ('10','11','20','21','31') then 1 else 0 end
     or month_int = case when m.mdcr_status_code_02 in ('10','11','20','21','31') then 2 else 0 end
     or month_int = case when m.mdcr_status_code_03 in ('10','11','20','21','31') then 3 else 0 end
     or month_int = case when m.mdcr_status_code_04 in ('10','11','20','21','31') then 4 else 0 end
     or month_int = case when m.mdcr_status_code_05 in ('10','11','20','21','31') then 5 else 0 end
     or month_int = case when m.mdcr_status_code_06 in ('10','11','20','21','31')then 6 else 0 end
     or month_int = case when m.mdcr_status_code_07 in ('10','11','20','21','31') then 7 else 0 end
     or month_int = case when m.mdcr_status_code_08 in ('10','11','20','21','31') then 8 else 0 end
     or month_int = case when m.mdcr_status_code_09 in ('10','11','20','21','31') then 9 else 0 end
     or month_int = case when m.mdcr_status_code_10 in ('10','11','20','21','31') then 10 else 0 end
     or month_int = case when m.mdcr_status_code_11 in ('10','11','20','21','31') then 11 else 0 end
     or month_int = case when m.mdcr_status_code_12 in ('10','11','20','21','31') then 12 else 0 end
    )
  join reference_tables.ref_medicare_entlmt_buyin ent 
    on ent.buyin_cd = case when b.month_int = 1 then m.mdcr_entlmt_buyin_ind_01 
                           when b.month_int = 2 then m.mdcr_entlmt_buyin_ind_02 
                           when b.month_int = 3 then m.mdcr_entlmt_buyin_ind_03 
                           when b.month_int = 4 then m.mdcr_entlmt_buyin_ind_04 
                           when b.month_int = 5 then m.mdcr_entlmt_buyin_ind_05 
                           when b.month_int = 6 then m.mdcr_entlmt_buyin_ind_06 
                           when b.month_int = 7 then m.mdcr_entlmt_buyin_ind_07 
                           when b.month_int = 8 then m.mdcr_entlmt_buyin_ind_08
                           when b.month_int = 9 then m.mdcr_entlmt_buyin_ind_09 
                           when b.month_int = 10 then m.mdcr_entlmt_buyin_ind_10 
                           when b.month_int = 11 then m.mdcr_entlmt_buyin_ind_11 
                           when b.month_int = 12 then m.mdcr_entlmt_buyin_ind_12 
                           else null end      
  join reference_tables.ref_medicare_ptd_cntrct ptd 
    on ptd.ptd_first_char = case when b.month_int = 1 then substring(m.ptd_cntrct_id_01,1,1)
                                 when b.month_int = 2 then substring(m.ptd_cntrct_id_02,1,1)
                                 when b.month_int = 3 then substring(m.ptd_cntrct_id_03,1,1)
                                 when b.month_int = 4 then substring(m.ptd_cntrct_id_04,1,1)
                                 when b.month_int = 5 then substring(m.ptd_cntrct_id_05,1,1)
                                 when b.month_int = 6 then substring(m.ptd_cntrct_id_06,1,1)
                                 when b.month_int = 7 then substring(m.ptd_cntrct_id_07,1,1)
                                 when b.month_int = 8 then substring(m.ptd_cntrct_id_08,1,1)
                                 when b.month_int = 9 then substring(m.ptd_cntrct_id_09,1,1)
                                 when b.month_int = 10 then substring(m.ptd_cntrct_id_10,1,1)
                                 when b.month_int = 11 then substring(m.ptd_cntrct_id_11,1,1)
                                 when b.month_int = 12 then substring(m.ptd_cntrct_id_12,1,1)
                           else null end
;
	



-- Medicare National --------------------------------------------------------------------------------------
delete from dw_staging.member_enrollment_monthly where data_source = 'mcrt';

insert into dw_staging.member_enrollment_monthly (
	data_source, year, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd, rx_coverage  ,fiscal_year , race_cd    
	)	
select 'mcrn',b.year_int, b.month_year_id, a.uth_member_id,
	   c.gender_cd,case when e.state_cd is null then 'XX' else e.state_cd end, m.zip_cd, substring(m.zip_cd,1,3),
	   bene_enrollmt_ref_yr::int - extract( year from bene_birth_dt::date),bene_birth_dt::date, bene_death_dt::date,
	   ent.plan_type, 'MDCR', ptd.ptd_coverage, m.year::int2, r.race_cd
from medicare_national.mbsf_abcd_summary m
  join data_warehouse.dim_uth_member_id a
    on a.member_id_src = m.bene_id::text
   and a.data_source = 'mcrn'
  left outer join reference_tables.ref_gender c
    on c.data_source = 'mcr'
   and c.gender_cd_src = m.sex_ident_cd
  left outer join reference_tables.ref_medicare_state_codes e 
     on e.medicare_state_cd = m.state_code   
  left outer join reference_tables.ref_race r 
     on r.race_cd_src = m.bene_race_cd 
    and r.data_source = 'mcrn'
  join reference_tables.ref_month_year b
    on b.year_int = bene_enrollmt_ref_yr::int
   and 
   (	month_int = case when m.mdcr_entlmt_buyin_ind_01 in ('1','3','A','C') then 1 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_02 in ('1','3','A','C') then 2 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_03 in ('1','3','A','C') then 3 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_04 in ('1','3','A','C') then 4 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_05 in ('1','3','A','C') then 5 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_06 in ('1','3','A','C') then 6 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_07 in ('1','3','A','C') then 7 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_08 in ('1','3','A','C') then 8 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_09 in ('1','3','A','C') then 9 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_10 in ('1','3','A','C') then 10 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_11 in ('1','3','A','C') then 11 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_12 in ('1','3','A','C') then 12 else 0 end
    )
  join reference_tables.ref_medicare_entlmt_buyin ent 
    on ent.buyin_cd = case when b.month_int = 1 then m.mdcr_entlmt_buyin_ind_01 
                           when b.month_int = 2 then m.mdcr_entlmt_buyin_ind_02 
                           when b.month_int = 3 then m.mdcr_entlmt_buyin_ind_03 
                           when b.month_int = 4 then m.mdcr_entlmt_buyin_ind_04 
                           when b.month_int = 5 then m.mdcr_entlmt_buyin_ind_05 
                           when b.month_int = 6 then m.mdcr_entlmt_buyin_ind_06 
                           when b.month_int = 7 then m.mdcr_entlmt_buyin_ind_07 
                           when b.month_int = 8 then m.mdcr_entlmt_buyin_ind_08
                           when b.month_int = 9 then m.mdcr_entlmt_buyin_ind_09 
                           when b.month_int = 10 then m.mdcr_entlmt_buyin_ind_10 
                           when b.month_int = 11 then m.mdcr_entlmt_buyin_ind_11 
                           when b.month_int = 12 then m.mdcr_entlmt_buyin_ind_12 
                           else null end      
  join reference_tables.ref_medicare_ptd_cntrct ptd 
    on ptd.ptd_first_char = case when b.month_int = 1 then substring(m.ptd_cntrct_id_01,1,1)
                                 when b.month_int = 2 then substring(m.ptd_cntrct_id_02,1,1)
                                 when b.month_int = 3 then substring(m.ptd_cntrct_id_03,1,1)
                                 when b.month_int = 4 then substring(m.ptd_cntrct_id_04,1,1)
                                 when b.month_int = 5 then substring(m.ptd_cntrct_id_05,1,1)
                                 when b.month_int = 6 then substring(m.ptd_cntrct_id_06,1,1)
                                 when b.month_int = 7 then substring(m.ptd_cntrct_id_07,1,1)
                                 when b.month_int = 8 then substring(m.ptd_cntrct_id_08,1,1)
                                 when b.month_int = 9 then substring(m.ptd_cntrct_id_09,1,1)
                                 when b.month_int = 10 then substring(m.ptd_cntrct_id_10,1,1)
                                 when b.month_int = 11 then substring(m.ptd_cntrct_id_11,1,1)
                                 when b.month_int = 12 then substring(m.ptd_cntrct_id_12,1,1)
                           else null end
;





--- ***** Medicaid *****
delete from dw_staging.member_enrollment_monthly where data_source = 'mdcd';

insert into dw_staging.member_enrollment_monthly (
	data_source, year, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd, rx_coverage, fiscal_year, race_cd   
	)		
select 'mdcd', substring(elig_date,1,4)::int2 as year, elig_date::int as my, b.uth_member_id, 
       a.sex, z.state, a.zip, substring(a.zip,1,3) as zip3, 
       floor(a.age::float), a.dob::date, null as dth, 
       null as plan_type, 'MCD' as bus, 1 as rx, year_fy , r.race_cd
from medicaid.enrl  a 
  join data_warehouse.dim_uth_member_id b  
     on b.data_source = 'mdcd'
    and b.member_id_src = a.client_nbr 
  left outer join reference_tables.ref_zip_code z 
     on a.zip = z.zip 
  left outer join reference_tables.medicaid_lu_contract c 
     on c.plan_cd = a.contract_id 
  left outer join reference_tables.ref_race r 
     on r.race_cd_src = a.race 
    and r.data_source = 'mdcd'
;

---medicaid chip
insert into dw_staging.member_enrollment_monthly(
	data_source, year, month_year_id, uth_member_id,
	gender_cd, state, zip5, zip3,
	age_derived, dob_derived, death_date,
	plan_type, bus_cd, rx_coverage  ,fiscal_year, race_cd   
	)	
select 'mdcd', substring(elig_month,1,4)::int2 as year, elig_month::int as my, b.uth_member_id, 
       a.gender_cd , z.state,  substring(a.mailing_zip,1,5) , substring(a.mailing_zip,1,3) as zip3, 
       floor(a.age::float), to_date( substring(date_of_birth,6,4) || substring(date_of_birth,3,3) || substring(date_of_birth,1,2) ,'YYYYMonDD') as dob, null as dth, 
       null as plan_type, 'MCD' as bus, 1 as rx, year_fy , r.race_cd
from medicaid.chip_uth  a 
  join data_warehouse.dim_uth_member_id b  
     on b.data_source = 'mdcd'
    and b.member_id_src = a.client_nbr 
  left outer join reference_tables.ref_zip_code z 
     on  substring(a.mailing_zip,1,5) = z.zip 
  left outer join reference_tables.medicaid_lu_contract c 
     on c.plan_cd = a.plan_cd 
  left outer join reference_tables.ref_race r 
     on r.race_cd_src = a.ethnicity 
    and r.data_source = 'mdcd'
;


-------(^---------------- data loads --------------------^)



------*****run below code for data sources***** vvvvvvvvvvvvvvvvvvvvvvvvvvvvvv

---redistribute table on row id so below scripts run quicker
create table dw_staging.temp_member_enrollment_monthly
with (appendonly=true, orientation=column) as 
select * from dw_staging.member_enrollment_monthly 
distributed by (row_id);

drop table if exists dw_staging.member_enrollment_monthly; 

alter table dw_staging.temp_member_enrollment_monthly rename to dw_staging.member_enrollment_monthly;

vacuum analyze dw_staging.member_enrollment_monthly;



---- get row id of duplicate rows
drop table if exists  dev.temp_dupe_enrollment_rows;

create table dev.temp_dupe_enrollment_rows
with (appendonly=true, orientation=column) as 
select row_id::bigint as row_id 
from
(		
select row_number() over(partition by uth_member_id, month_year_id order by month_year_id) as rn
	      ,*
	from dw_staging.member_enrollment_monthly 		
) sub
where rn > 1
distributed by (row_id);	

--remote dupe records so only one per member per month - runtime: 12min
delete from dw_staging.member_enrollment_monthly a 
  using dev.temp_dupe_enrollment_rows b 
   where a.row_id = b.row_id 
; 
 	
		
---**script to build consecutive enrolled months	  16min	
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



--update consec enrolled months  9m 
update dw_staging.member_enrollment_monthly a set consecutive_enrolled_months = b.in_streak 
from dev.temp_consec_enrollment b 
where a.row_id = b.row_id
;

-----*****run above code for all data sets*****              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

--final
vacuum analyze dw_staging.member_enrollment_monthly;

----/END SCRIPT


select data_source, year, count(*)
from dw_staging.member_enrollment_monthly
group by data_source, year
order by data_source, year
;



