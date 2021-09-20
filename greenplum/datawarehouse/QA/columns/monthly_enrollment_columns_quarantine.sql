/*
 *
 *
--------------------------------------------------------------------------------

--********************************************----------------------------------
--------Claim header QA quarantine table
--********************************************----------------------------------

--------------------------------------------------------------------------------

test each variable,
if some have a different correct value, do those seperately
quarantine incorrect values into one table with flag for that variable

--- jw001 | 8/12/21 | script creation

*/

--- quarantine table


drop table if exists qa_reporting.monthly_enroll_col_quarantine;

create table qa_reporting.monthly_enroll_col_quarantine 
with(appendonly = true, orientation = column, compresstype = zlib)
as
select *
from data_warehouse.member_enrollment_monthly
limit 0 
distributed by (uth_member_id);


------------------------------------
--------data source---------
------------------------------------

alter table qa_reporting.monthly_enroll_col_quarantine 
  add column data_source_flag INT; 

insert into qa_reporting.monthly_enroll_col_quarantine 
            (data_source, 
             "year", 
             month_year_id, 
             uth_member_id, 
             consecutive_enrolled_months, 
             gender_cd, 
             state, 
             zip5, 
             zip3, 
             age_derived, 
             dob_derived, 
             death_date, 
             plan_type, 
             bus_cd, 
             employee_status, 
             claim_created_flag, 
             row_identifier, 
             rx_coverage, 
             fiscal_year, 
             race_cd, 
             data_source_flag) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as data_source_flag 
from   data_warehouse.member_enrollment_monthly 
where  data_source not in ( 'mcrt', 'optz', 'mdcd', 'mcrn', 
                            'truv', 'optd' );   

------------------------------------
--------year--------------
------------------------------------
alter table qa_reporting.monthly_enroll_col_quarantine 
  add column year_flag INT; 

insert into qa_reporting.monthly_enroll_col_quarantine 
            (data_source, 
             "year", 
             month_year_id, 
             uth_member_id, 
             consecutive_enrolled_months, 
             gender_cd, 
             state, 
             zip5, 
             zip3, 
             age_derived, 
             dob_derived, 
             death_date, 
             plan_type, 
             bus_cd, 
             employee_status, 
             claim_created_flag, 
             row_identifier, 
             rx_coverage, 
             fiscal_year, 
             race_cd, 
             year_flag) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as year_flag 
from   data_warehouse.member_enrollment_monthly 
where  year not between 2007 and 2020 
        or year is null;   


------------------------------------
--------month_year_id--------------
------------------------------------
alter table qa_reporting.monthly_enroll_col_quarantine 
  add column month_year_flag INT; 

insert into qa_reporting.monthly_enroll_col_quarantine 
            (data_source, 
             "year", 
             month_year_id, 
             uth_member_id, 
             consecutive_enrolled_months, 
             gender_cd, 
             state, 
             zip5, 
             zip3, 
             age_derived, 
             dob_derived, 
             death_date, 
             plan_type, 
             bus_cd, 
             employee_status, 
             claim_created_flag, 
             row_identifier, 
             rx_coverage, 
             fiscal_year, 
             race_cd, 
             month_year_flag) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as month_year_flag 
from   data_warehouse.member_enrollment_monthly 
where  month_year_id not between 200701 and 202012 
        or month_year_id is null;   

    
    
---------------------------------   
-----uth_member_id---------------
---------------------------------
    
-------------------------check 1: in dim table  
alter table qa_reporting.monthly_enroll_col_quarantine add column id_not_in_dim int;

with ut_id_table
as (
    select a.uth_member_id, 
    				a."year", 
    				a.data_source, 
    				b.uth_member_id as dim_id
    from data_warehouse.member_enrollment_monthly a
    left join data_warehouse.dim_uth_member_id b on a.uth_member_id = b.uth_member_id
    )
insert into qa_reporting.monthly_enroll_col_quarantine 
            ( 
                        data_source, 
                        "year", 
                        month_year_id, 
                        uth_member_id, 
                        consecutive_enrolled_months, 
                        gender_cd, 
                        state, 
                        zip5, 
                        zip3, 
                        age_derived, 
                        dob_derived, 
                        death_date, 
                        plan_type, 
                        bus_cd, 
                        employee_status, 
                        claim_created_flag, 
                        row_identifier, 
                        rx_coverage, 
                        fiscal_year, 
                        race_cd, 
                        id_not_in_dim 
            ) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as id_not_in_dim 
from   ut_id_table 
where  dim_id is null ;  

------------check 2 in src table


/*

    select distinct patidsrc
    into dev.jw_ids_src
    from (
        select patid::text as patidsrc
        from optum_zip.mbr_enroll_r

        union all

        select patid::text as patidsrc
        from optum_zip.mbr_enroll

        union all

        select enrolid::text as patidsrc
        from truven.ccaet

        union all

        select enrolid::text as patidsrc
        from truven.mdcrt

        union all

        select bene_id::text as patidsrc
        from medicare_texas.mbsf_abcd_summary
        
        union all
        
        select bene_id::text as patidsrc
        from medicare_national.mbsf_abcd_summary
        
        union all
        
        select client_nbr::text as patidsrc
        from   medicaid.enrl
        
        union all
        
        select client_nbr::text as patidsrc
        from medicaid.chip_uth
        
        ) a;     
    
    */

alter table qa_reporting.monthly_enroll_col_quarantine add column id_not_in_source int;

with ut_id_table as 
( 
    select a.uth_member_id, a."year", a.data_source, b.member_id_src as dim_id
    from data_warehouse.member_enrollment_monthly a
    join data_warehouse.dim_uth_member_id b 
        on a.uth_member_id = b.uth_member_id
    left outer join dev.qa_temp_ids_src c 
        on b.member_id_src = c.patidsrc
) 
insert into qa_reporting.monthly_enroll_col_quarantine 
            ( 
                        data_source, 
                        "year", 
                        month_year_id, 
                        uth_member_id, 
                        consecutive_enrolled_months, 
                        gender_cd, 
                        state, 
                        zip5, 
                        zip3, 
                        age_derived, 
                        dob_derived, 
                        death_date, 
                        plan_type, 
                        bus_cd, 
                        employee_status, 
                        claim_created_flag, 
                        row_identifier, 
                        rx_coverage, 
                        fiscal_year, 
                        race_cd, 
                        id_not_in_source 
            ) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as id_not_in_source 
from   ut_id_table 
where  dim_id is null ;  

------------------------------------
--------consecutive enrolled months---------change max range to # of years times 12 when new year added -----
------------------------------------

alter table qa_reporting.monthly_enroll_col_quarantine 
  add column consecutive_flag INT; 

insert into qa_reporting.monthly_enroll_col_quarantine 
            (data_source, 
             "year", 
             month_year_id, 
             uth_member_id, 
             consecutive_enrolled_months, 
             gender_cd, 
             state, 
             zip5, 
             zip3, 
             age_derived, 
             dob_derived, 
             death_date, 
             plan_type, 
             bus_cd, 
             employee_status, 
             claim_created_flag, 
             row_identifier, 
             rx_coverage, 
             fiscal_year, 
             race_cd, 
             consecutive_flag) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as consecutive_flag 
from   data_warehouse.member_enrollment_monthly 
where  consecutive_enrolled_months not between 0 and 156
        or consecutive_enrolled_months is null;   
    
                   
------------------------------------
--------gender_cd--------------
------------------------------------

alter table qa_reporting.monthly_enroll_col_quarantine 
  add column gender_cd_flag INT; 

insert into qa_reporting.monthly_enroll_col_quarantine 
            (data_source, 
             "year", 
             month_year_id, 
             uth_member_id, 
             consecutive_enrolled_months, 
             gender_cd, 
             state, 
             zip5, 
             zip3, 
             age_derived, 
             dob_derived, 
             death_date, 
             plan_type, 
             bus_cd, 
             employee_status, 
             claim_created_flag, 
             row_identifier, 
             rx_coverage, 
             fiscal_year, 
             race_cd, 
             gender_cd_flag) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as gender_cd_flag 
from   data_warehouse.member_enrollment_monthly 
where  gender_cd not in ( 'M', 'F', 'U' ) 
        or gender_cd is null;   

-----------------------------------
-----state
------------------------------------
alter table qa_reporting.monthly_enroll_col_quarantine add column state_flag int;

with state_table as 
( 
	select a.*, b.state as state_check
	from data_warehouse.member_enrollment_monthly a
	left join dev.qa_temp_all_states b on a.state = b.state
          ) 
insert into qa_reporting.monthly_enroll_col_quarantine 
            ( 
                        data_source, 
                        "year", 
                        month_year_id, 
                        uth_member_id, 
                        consecutive_enrolled_months, 
                        gender_cd, 
                        state, 
                        zip5, 
                        zip3, 
                        age_derived, 
                        dob_derived, 
                        death_date, 
                        plan_type, 
                        bus_cd, 
                        employee_status, 
                        claim_created_flag, 
                        row_identifier, 
                        rx_coverage, 
                        fiscal_year, 
                        race_cd, 
                        state_flag 
            ) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as state_flag 
from   data_warehouse.member_enrollment_monthly 
where  state_check is null and zip5 <> '00000';  




-----------------------------------
-----zip5
------------------------------------




--------zip5---------

alter table qa_reporting.monthly_enroll_col_quarantine 
  add column zip5_flag INT; 

insert into qa_reporting.monthly_enroll_col_quarantine 
            (data_source, 
             "year", 
             month_year_id, 
             uth_member_id, 
             consecutive_enrolled_months, 
             gender_cd, 
             state, 
             zip5, 
             zip3, 
             age_derived, 
             dob_derived, 
             death_date, 
             plan_type, 
             bus_cd, 
             employee_status, 
             claim_created_flag, 
             row_identifier, 
             rx_coverage, 
             fiscal_year, 
             race_cd, 
             zip5_flag) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as zip5_flag 
from   data_warehouse.member_enrollment_monthly 
where  ( zip5 !~ '^\d{5}$' 
        or zip5 is null )
           and data_source in ( 'mcrn', 'mdcd', 'optz', 'mcrt' );   

--------zip5---------

insert into qa_reporting.monthly_enroll_col_quarantine 
            (data_source, 
             "year", 
             month_year_id, 
             uth_member_id, 
             consecutive_enrolled_months, 
             gender_cd, 
             state, 
             zip5, 
             zip3, 
             age_derived, 
             dob_derived, 
             death_date, 
             plan_type, 
             bus_cd, 
             employee_status, 
             claim_created_flag, 
             row_identifier, 
             rx_coverage, 
             fiscal_year, 
             race_cd, 
             zip5_flag) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as zip5_flag 
from   data_warehouse.member_enrollment_monthly 
where  zip5 is not null 
       and data_source in ( 'truv', 'optd' );   


-----------------------------------
-----zip3
------------------------------------
   
alter table qa_reporting.monthly_enroll_col_quarantine 
  add column zip3_flag INT; 
  
insert into qa_reporting.monthly_enroll_col_quarantine 
            (data_source, 
             "year", 
             month_year_id, 
             uth_member_id, 
             consecutive_enrolled_months, 
             gender_cd, 
             state, 
             zip5, 
             zip3, 
             age_derived, 
             dob_derived, 
             death_date, 
             plan_type, 
             bus_cd, 
             employee_status, 
             claim_created_flag, 
             row_identifier, 
             rx_coverage, 
             fiscal_year, 
             race_cd, 
             zip3_flag) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as zip3_flag 
from   data_warehouse.member_enrollment_monthly 
where   zip3 !~ '^\d{3}$'
       and data_source in ('mcrn', 'truv', 'mdcd', 'optz', 'mcrt');   


--------zip3-----optd----


insert into qa_reporting.monthly_enroll_col_quarantine 
            (data_source, 
             "year", 
             month_year_id, 
             uth_member_id, 
             consecutive_enrolled_months, 
             gender_cd, 
             state, 
             zip5, 
             zip3, 
             age_derived, 
             dob_derived, 
             death_date, 
             plan_type, 
             bus_cd, 
             employee_status, 
             claim_created_flag, 
             row_identifier, 
             rx_coverage, 
             fiscal_year, 
             race_cd, 
             zip3_flag) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as zip3_flag 
from   data_warehouse.member_enrollment_monthly 
where  zip3 is not null 
       and data_source = 'optd';   



-----------------------------------
-----age_derived
------------------------------------


--------age_derived----------
alter table qa_reporting.monthly_enroll_col_quarantine 
  add column age_derived_flag INT; 

insert into qa_reporting.monthly_enroll_col_quarantine 
            (data_source, 
             "year", 
             month_year_id, 
             uth_member_id, 
             consecutive_enrolled_months, 
             gender_cd, 
             state, 
             zip5, 
             zip3, 
             age_derived, 
             dob_derived, 
             death_date, 
             plan_type, 
             bus_cd, 
             employee_status, 
             claim_created_flag, 
             row_identifier, 
             rx_coverage, 
             fiscal_year, 
             race_cd, 
             age_derived_flag) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as age_derived_flag 
from   data_warehouse.member_enrollment_monthly 
where  age_derived not between 0 and 150 
        or age_derived is null;   


-----------------------------------
-----dob_derived
------------------------------------
select * from qa_reporting.monthly_enroll_col_quarantine where age_derived_flag = 1;

--------dob_derived----------
alter table qa_reporting.monthly_enroll_col_quarantine 
  add column dob_derived_flag INT; 

insert into qa_reporting.monthly_enroll_col_quarantine 
            (data_source, 
             "year", 
             month_year_id, 
             uth_member_id, 
             consecutive_enrolled_months, 
             gender_cd, 
             state, 
             zip5, 
             zip3, 
             age_derived, 
             dob_derived, 
             death_date, 
             plan_type, 
             bus_cd, 
             employee_status, 
             claim_created_flag, 
             row_identifier, 
             rx_coverage, 
             fiscal_year, 
             race_cd, 
             dob_derived_flag) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as age_dob_derived_flag 
from   data_warehouse.member_enrollment_monthly 
where  dob_derived not between '1800-01-01' and '2050-01-01' 
        or dob_derived is null;   


-----------------------------------
-----death_date
------------------------------------


--------death_date------mcrt mcrn optd----

alter table qa_reporting.monthly_enroll_col_quarantine 
  add column death_date_flag INT; 

insert into qa_reporting.monthly_enroll_col_quarantine 
            (data_source, 
             "year", 
             month_year_id, 
             uth_member_id, 
             consecutive_enrolled_months, 
             gender_cd, 
             state, 
             zip5, 
             zip3, 
             age_derived, 
             dob_derived, 
             death_date, 
             plan_type, 
             bus_cd, 
             employee_status, 
             claim_created_flag, 
             row_identifier, 
             rx_coverage, 
             fiscal_year, 
             race_cd, 
             death_date_flag) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as death_date_flag 
from   data_warehouse.member_enrollment_monthly 
where  death_date not between '1800-01-01' and '2050-01-01' 
       and data_source in ( 'mcrn', 'mcrt', 'optd' );  
   
   
--------death_date------mcrt mcrn optd----
   

insert into qa_reporting.monthly_enroll_col_quarantine 
            (data_source, 
             "year", 
             month_year_id, 
             uth_member_id, 
             consecutive_enrolled_months, 
             gender_cd, 
             state, 
             zip5, 
             zip3, 
             age_derived, 
             dob_derived, 
             death_date, 
             plan_type, 
             bus_cd, 
             employee_status, 
             claim_created_flag, 
             row_identifier, 
             rx_coverage, 
             fiscal_year, 
             race_cd, 
             death_date_flag) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as death_date_flag 
from   data_warehouse.member_enrollment_monthly 
where  death_date not between '1800-01-01' and '2050-01-01' 
       and data_source in ( 'mcrn', 'mcrt', 'optd' );   


-----------------------------------
-----plan_type
------------------------------------

alter table qa_reporting.monthly_enroll_col_quarantine 
  add column plan_type_flag INT; 
  

--------plan_type------ optz optd---- 
insert into qa_reporting.monthly_enroll_col_quarantine 
            (data_source, 
             "year", 
             month_year_id, 
             uth_member_id, 
             consecutive_enrolled_months, 
             gender_cd, 
             state, 
             zip5, 
             zip3, 
             age_derived, 
             dob_derived, 
             death_date, 
             plan_type, 
             bus_cd, 
             employee_status, 
             claim_created_flag, 
             row_identifier, 
             rx_coverage, 
             fiscal_year, 
             race_cd, 
             plan_type_flag) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as plan_type_flag 
from   data_warehouse.member_enrollment_monthly 
where  (plan_type not in ( 'ALL', 'EPO', 'GPO', 'HMO', 
                          'IND', 'IPP', 'NONE', 'OTH', 
                          'POS', 'PPO', 'SPN', 'UNK', 
                          'BMM', 'CMP', 'EPO', 'HMO', 
                          'POS', 'PPO', 'POS', 'CDHP', 'HDHP' ) 
        or plan_type is null )
           and data_source in ( 'optz', 'optd' );   

--------plan_type------ 'truv'---- 
insert into qa_reporting.monthly_enroll_col_quarantine 
            (data_source, 
             "year", 
             month_year_id, 
             uth_member_id, 
             consecutive_enrolled_months, 
             gender_cd, 
             state, 
             zip5, 
             zip3, 
             age_derived, 
             dob_derived, 
             death_date, 
             plan_type, 
             bus_cd, 
             employee_status, 
             claim_created_flag, 
             row_identifier, 
             rx_coverage, 
             fiscal_year, 
             race_cd, 
             plan_type_flag) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as plan_type_flag 
from   data_warehouse.member_enrollment_monthly 
where  (plan_type not in ( 'ALL', 'EPO', 'GPO', 'HMO', 
                          'IND', 'IPP', 'NONE', 'OTH', 
                          'POS', 'PPO', 'SPN', 'UNK', 
                          'BMM', 'CMP', 'EPO', 'HMO', 
                          'POS', 'PPO', 'POS', 'CDHP', 'HDHP' ) 
        and plan_type is not null )
           and data_source in ( 'truv' );   


--------plan_type------mcrn mcrt---
       
  insert into qa_reporting.monthly_enroll_col_quarantine 
            (data_source, 
             "year", 
             month_year_id, 
             uth_member_id, 
             consecutive_enrolled_months, 
             gender_cd, 
             state, 
             zip5, 
             zip3, 
             age_derived, 
             dob_derived, 
             death_date, 
             plan_type, 
             bus_cd, 
             employee_status, 
             claim_created_flag, 
             row_identifier, 
             rx_coverage, 
             fiscal_year, 
             race_cd, 
             plan_type_flag) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as plan_type_flag 
from   data_warehouse.member_enrollment_monthly 
where   ( plan_type not in ( 'AB', 'B', 'A', 'AB', 
                          'A', 'B' ) 
        or plan_type is null )
           and data_source in ( 'mcrn', 'mcrt' );   

--------plan_type------mdcd----

insert into qa_reporting.monthly_enroll_col_quarantine 
            (data_source, 
             "year", 
             month_year_id, 
             uth_member_id, 
             consecutive_enrolled_months, 
             gender_cd, 
             state, 
             zip5, 
             zip3, 
             age_derived, 
             dob_derived, 
             death_date, 
             plan_type, 
             bus_cd, 
             employee_status, 
             claim_created_flag, 
             row_identifier, 
             rx_coverage, 
             fiscal_year, 
             race_cd, 
             plan_type_flag) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as plan_type_flag 
from   data_warehouse.member_enrollment_monthly 
where  (plan_type is not null 
       and data_source = 'mdcd' );   



-----------------------------------
-----bus_cd
------------------------------------
   
   alter table qa_reporting.monthly_enroll_col_quarantine 
  add column bus_cd_flag INT; 

 insert into qa_reporting.monthly_enroll_col_quarantine 
            (data_source, 
             "year", 
             month_year_id, 
             uth_member_id, 
             consecutive_enrolled_months, 
             gender_cd, 
             state, 
             zip5, 
             zip3, 
             age_derived, 
             dob_derived, 
             death_date, 
             plan_type, 
             bus_cd, 
             employee_status, 
             claim_created_flag, 
             row_identifier, 
             rx_coverage, 
             fiscal_year, 
             race_cd, 
             bus_cd_flag) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as bus_cd_flag 
from   data_warehouse.member_enrollment_monthly 
where  bus_cd not in ('COM', 'MDCR', 'MCR', 'MCD')
                        or bus_cd is null;  
                
                
                
-----------------------------------
-----employee_status
------------------------------------


alter table qa_reporting.monthly_enroll_col_quarantine
add column employee_status_flag int;

                
                
-------------truven-----------------------

insert into qa_reporting.monthly_enroll_col_quarantine 
            ( 
                        data_source, 
                        "year", 
                        month_year_id, 
                        uth_member_id, 
                        consecutive_enrolled_months, 
                        gender_cd, 
                        state, 
                        zip5, 
                        zip3, 
                        age_derived, 
                        dob_derived, 
                        death_date, 
                        plan_type, 
                        bus_cd, 
                        employee_status, 
                        claim_created_flag, 
                        row_identifier, 
                        rx_coverage, 
                        fiscal_year, 
                        race_cd, 
                        employee_status_flag 
            ) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as employee_status_flag 
from   data_warehouse.member_enrollment_monthly 
where  employee_status not between '1' and '9' 
or     employee_status is null 
and  data_source = 'truv' ;  


------------------------------------


insert into qa_reporting.monthly_enroll_col_quarantine 
            (data_source, 
             "year", 
             month_year_id, 
             uth_member_id, 
             consecutive_enrolled_months, 
             gender_cd, 
             state, 
             zip5, 
             zip3, 
             age_derived, 
             dob_derived, 
             death_date, 
             plan_type, 
             bus_cd, 
             employee_status, 
             claim_created_flag, 
             row_identifier, 
             rx_coverage, 
             fiscal_year, 
             race_cd, 
             employee_status_flag) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as employee_status_flag 
from   data_warehouse.member_enrollment_monthly 
where  employee_status is not null 
       and data_source in ( 'mcrn', 'optd', 'mdcd', 'optz', 'mcrt' );   

-----------------------------------
-----claim_created_flag
------------------------------------

    
    -------------------not created yet 


-----------------------------------
-----row_identifier
------------------------------------

alter table qa_reporting.monthly_enroll_col_quarantine 
  add column row_identifier_flag INT; 

insert into qa_reporting.monthly_enroll_col_quarantine 
            (data_source, 
             "year", 
             month_year_id, 
             uth_member_id, 
             consecutive_enrolled_months, 
             gender_cd, 
             state, 
             zip5, 
             zip3, 
             age_derived, 
             dob_derived, 
             death_date, 
             plan_type, 
             bus_cd, 
             employee_status, 
             claim_created_flag, 
             row_identifier, 
             rx_coverage, 
             fiscal_year, 
             race_cd, 
             row_identifier_flag) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as row_identifier_flag 
from   data_warehouse.member_enrollment_monthly 
where  Pg_typeof(row_identifier) :: text not like 'bigint' 
        or row_identifier is null;   
    
    
    
    
    
    
    
    
    
    
    
    
    


-----------------------------------
-----rx_coverage
------------------------------------

alter table qa_reporting.monthly_enroll_col_quarantine 
  add column rx_coverage_flag INT; 

insert into qa_reporting.monthly_enroll_col_quarantine 
            (data_source, 
             "year", 
             month_year_id, 
             uth_member_id, 
             consecutive_enrolled_months, 
             gender_cd, 
             state, 
             zip5, 
             zip3, 
             age_derived, 
             dob_derived, 
             death_date, 
             plan_type, 
             bus_cd, 
             employee_status, 
             claim_created_flag, 
             row_identifier, 
             rx_coverage, 
             fiscal_year, 
             race_cd, 
             rx_coverage_flag) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as rx_coverage_flag 
from   data_warehouse.member_enrollment_monthly 
where  rx_coverage not between 0 and 1 
        or rx_coverage is null;   


-----------------------------------
-----fiscal_year
------------------------------------

  alter table qa_reporting.monthly_enroll_col_quarantine 
  add column fiscal_year_flag INT; 

insert into qa_reporting.monthly_enroll_col_quarantine 
            (data_source, 
             "year", 
             month_year_id, 
             uth_member_id, 
             consecutive_enrolled_months, 
             gender_cd, 
             state, 
             zip5, 
             zip3, 
             age_derived, 
             dob_derived, 
             death_date, 
             plan_type, 
             bus_cd, 
             employee_status, 
             claim_created_flag, 
             row_identifier, 
             rx_coverage, 
             fiscal_year, 
             race_cd, 
             fiscal_year_flag) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as fiscal_year_flag 
from   data_warehouse.member_enrollment_monthly 
where  fiscal_year not between 2007 and 2020 
        or fiscal_year is null;   


-----------------------------------
-----race_cd
------------------------------------

alter table qa_reporting.monthly_enroll_col_quarantine add column race_cd_flag int;

insert into qa_reporting.monthly_enroll_col_quarantine 
            ( 
                        data_source, 
                        "year", 
                        month_year_id, 
                        uth_member_id, 
                        consecutive_enrolled_months, 
                        gender_cd, 
                        state, 
                        zip5, 
                        zip3, 
                        age_derived, 
                        dob_derived, 
                        death_date, 
                        plan_type, 
                        bus_cd, 
                        employee_status, 
                        claim_created_flag, 
                        row_identifier, 
                        rx_coverage, 
                        fiscal_year, 
                        race_cd, 
                        race_cd_flag 
            ) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as race_cd_flag 
from   data_warehouse.member_enrollment_monthly 
where  race_cd::int not between 0 and 6 
or     race_cd is null 
and    data_source in ('mcrn', 
                       'optd', 
                       'mdcd', 
                       'mcrt') ;  


------optz--------truv----------
                   
insert into qa_reporting.monthly_enroll_col_quarantine 
            (data_source, 
             "year", 
             month_year_id, 
             uth_member_id, 
             consecutive_enrolled_months, 
             gender_cd, 
             state, 
             zip5, 
             zip3, 
             age_derived, 
             dob_derived, 
             death_date, 
             plan_type, 
             bus_cd, 
             employee_status, 
             claim_created_flag, 
             row_identifier, 
             rx_coverage, 
             fiscal_year, 
             race_cd, 
             race_cd_flag) 
select data_source, 
       "year", 
       month_year_id, 
       uth_member_id, 
       consecutive_enrolled_months, 
       gender_cd, 
       state, 
       zip5, 
       zip3, 
       age_derived, 
       dob_derived, 
       death_date, 
       plan_type, 
       bus_cd, 
       employee_status, 
       claim_created_flag, 
       row_identifier, 
       rx_coverage, 
       fiscal_year, 
       race_cd, 
       1 as race_cd_flag 
from   data_warehouse.member_enrollment_monthly 
where  race_cd is not null 
       and data_source in ( 'optz', 'truv' );   
       
   vacuum analyze qa_reporting.monthly_enroll_col_quarantine;
   
   
   
   select * from qa_reporting.monthly_enrollment_column_checks 
   order by test_var, data_source, "year";

  



select * from qa_reporting.monthly_enroll_col_quarantine where data_source_flag= 1 ;
select * from qa_reporting.monthly_enroll_col_quarantine where year_flag= 1 ;
select * from qa_reporting.monthly_enroll_col_quarantine where month_year_flag= 1 ;
select * from qa_reporting.monthly_enroll_col_quarantine where id_not_in_dim= 1 ;
select * from qa_reporting.monthly_enroll_col_quarantine where id_not_in_source= 1 ;
select * from qa_reporting.monthly_enroll_col_quarantine where consecutive_flag= 1 ;
select * from qa_reporting.monthly_enroll_col_quarantine where gender_cd_flag= 1 ;
select * from qa_reporting.monthly_enroll_col_quarantine where state_flag= 1 ;
select * from qa_reporting.monthly_enroll_col_quarantine where zip5_flag= 1 ;
select * from qa_reporting.monthly_enroll_col_quarantine where zip3_flag= 1 ;
select * from qa_reporting.monthly_enroll_col_quarantine where age_derived_flag= 1 ;
select * from qa_reporting.monthly_enroll_col_quarantine where dob_derived_flag= 1 ;
select * from qa_reporting.monthly_enroll_col_quarantine where death_date_flag= 1 ;
select * from qa_reporting.monthly_enroll_col_quarantine where plan_type_flag= 1 ;
select * from qa_reporting.monthly_enroll_col_quarantine where bus_cd_flag= 1 ;
select * from qa_reporting.monthly_enroll_col_quarantine where employee_status_flag= 1 ;
select * from qa_reporting.monthly_enroll_col_quarantine where row_identifier_flag= 1 ;
select * from qa_reporting.monthly_enroll_col_quarantine where rx_coverage_flag= 1 ;
select * from qa_reporting.monthly_enroll_col_quarantine where fiscal_year_flag= 1 ;
select * from qa_reporting.monthly_enroll_col_quarantine where race_cd_flag= 1 ;





   