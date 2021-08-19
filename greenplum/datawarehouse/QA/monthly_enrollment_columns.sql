/*
 *
 *
--------------------------------------------------------------------------------
--********************************************----------------------------------
--------   data_warehouse.member_enrollment_monthly Column QA ------------------
--********************************************----------------------------------
--------------------------------------------------------------------------------

--- jw001 | 8/16/21 | script creation

*/


drop table if exists qa_reporting.monthly_enrollment_column_checks ;

create table qa_reporting.monthly_enrollment_column_checks 
  ( 
     test_var        UNKNOWN null, 
     validvalues     INT8 null, 
     invalidvalues   INT8 null, 
     percent_invalid NUMERIC null, 
     pass_threshold  BOOL null, 
     "year"          INT2 null, 
     data_source     BPCHAR(4) null, 
     note            text null 
  )  ;


------------------------------------
--------data source---------
------------------------------------


insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'data_source' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select sum(case
				when data_source in ('mcrt', 'optz', 'mdcd', 'mcrn', 'truv', 'optd')
					then 1
				end) as validvalues,
		coalesce(sum(case
					when data_source not in ('mcrt', 'optz', 'mdcd', 'mcrn', 'truv', 'optd')
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from data_warehouse.member_enrollment_monthly
	group by 4,
		3
	) a;


------------------------------------
--------year--------------
------------------------------------


insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'year' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as notes
from (
	select sum(case
				when year between 2007 and 2020
					then 1
				end) as validvalues,
		coalesce(sum(case
					when year not between 2007 and 2020
						or year is null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from data_warehouse.member_enrollment_monthly
	group by 4,
		3
	) a;


------------------------------------
--------month_year_id--------------
------------------------------------


insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'month_year_id' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select sum(case
				when month_year_id between 200701 and 202012
					then 1
				end) as validvalues,
		coalesce(sum(case
					when month_year_id not between 200701 and 202012
						or month_year_id is null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from data_warehouse.member_enrollment_monthly
	group by 4,
		3
	) a;


	-- if time come and do casts
	
	
	
	
---------------------------------	
-----uth_member_id---------------
---------------------------------
	
-------------------------check 1: in dim table	
with ut_id_table
as (
    select a.uth_member_id, a."year", a.data_source 
    from data_warehouse.member_enrollment_monthly a
    left join data_warehouse.dim_uth_member_id b on a.uth_member_id = b.uth_member_id
    )
insert into qa_reporting.monthly_enrollment_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'uth_member_id' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    'validate in data_warehouse.dim_uth_member_id' as note
from (
    select sum(case
                when uth_member_id is not null
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when uth_member_id is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from ut_id_table
    group by 4,
        3
    ) a;


------------check 2 in src table


drop table if exists dev.qa_temp_ids_src;

    select distinct patidsrc
    into dev.qa_temp_ids_src
    from (
        select patid::text as patidsrc
        from optum_dod.mbr_enroll_r

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
    
    
with ut_id_table
as (
    select a.uth_member_id, a."year", a.data_source, b.member_id_src
    from data_warehouse.member_enrollment_monthly a
    join data_warehouse.dim_uth_member_id b 
        on a.uth_member_id = b.uth_member_id
    left outer join dev.qa_temp_ids_src c 
        on b.member_id_src = c.patidsrc
    )    
insert into qa_reporting.monthly_enrollment_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'uth_member_id' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    'validate in source tables' as note
from (
    select sum(case
                when uth_member_id is not null
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when uth_member_id is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from ut_id_table
    group by 4,
        3
    ) a;



------------------------------------
--------consecutive enrolled months---------change max range to # of years times 12 when new year added -----
------------------------------------

insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'consecutive_enrolled_months' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as notes
from (
	select sum(case
				when consecutive_enrolled_months between 0 and 168
					then 1
				end) as validvalues,
		coalesce(sum(case
					when consecutive_enrolled_months not between 0 and 168
						or consecutive_enrolled_months is null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from data_warehouse.member_enrollment_monthly
	group by 4,
		3
	) a;

	
------------------------------------
--------gender_cd--------------
------------------------------------


insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'gender_cd' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select sum(case
				when gender_cd in ('M', 'F', 'U')
					then 1
				end) as validvalues,
		coalesce(sum(case
					when gender_cd not in ('M', 'F', 'U')
						or gender_cd is null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from data_warehouse.member_enrollment_monthly
	group by 4,
		3
	) a;


-----------------------------------
-----state
------------------------------------

	
drop table if exists dev.qa_temp_all_states;

	select distinct state
	into dev.qa_temp_all_states
	from (
		select state as state
		from reference_tables.ref_zip_crosswalk

		union all

		select abbr as state
		from reference_tables.ref_truven_state_codes

		union all

		select state as state
		from optum_dod.mbr_enroll_r

		union all

		select state_cd as state
		from reference_tables.ref_medicare_state_codes

		union all

		select state as state
		from reference_tables.ref_zip_code
		) a;

		
with state_table
as (
	select a.*
	from data_warehouse.member_enrollment_monthly a
	left join dev.qa_temp_all_states b on a.state = b.state
	)
insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'state' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select sum(case
				when state is not null
					then 1
				end) as validvalues,
		coalesce(sum(case
					when state is null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from state_table
	group by 4,
		3
	) a;


-----------------------------------
-----zip5
------------------------------------

--------zip5---------
insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'zip5' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select sum(case
				when zip5 ~ '^\d{5}$'
					then 1
				end) as validvalues,
		coalesce(sum(case
					when zip5 !~ '^\d{5}$'
						or zip5 is null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from data_warehouse.member_enrollment_monthly
	where data_source in ('mcrn', 'mdcd', 'optz', 'mcrt')
	group by 4,
		3
	) a;

--------zip5---------

insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'zip5' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select sum(case
				when zip5 is null
					then 1
				end) as validvalues,
		coalesce(sum(case
					when zip5 is not null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from data_warehouse.member_enrollment_monthly
	where data_source in ('truv', 'optd')
	group by 4,
		3
	) a;


-----------------------------------
-----zip3
------------------------------------


insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'zip3' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select sum(case
				when zip3 ~ '^\d{3}$'
					then 1
				end) as validvalues,
		coalesce(sum(case
					when zip3 !~ '^\d{3}$'
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from data_warehouse.member_enrollment_monthly
	where data_source in ('mcrn', 'truv', 'mdcd', 'optz', 'mcrt')
	group by 4,
		3
	) a;

--------zip3-----optd----

insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'zip3' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'null is valid value' as note
from (
	select sum(case
				when zip3 is null
					then 1
				end) as validvalues,
		coalesce(sum(case
					when zip3 is not null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from data_warehouse.member_enrollment_monthly
	where data_source = 'optd'
	group by 4,
		3
	) a;


	
-----------------------------------
-----age_derived
------------------------------------

	
--------age_derived----------

insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'age_derived' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as notes
from (
	select sum(case
				when age_derived between 0 and 150
					then 1
				end) as validvalues,
		coalesce(sum(case
					when age_derived not between 0 and 150
						or age_derived is null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from data_warehouse.member_enrollment_monthly
	group by 4,
		3
	) a;



-----------------------------------
-----dob_derived
------------------------------------


--------dob_derived----------

insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'dob_derived' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select sum(case
				when dob_derived between '1800-01-01' and '2050-01-01'
					then 1
				end) as validvalues,
		coalesce(sum(case
					when dob_derived not between '1800-01-01' and '2050-01-01'
						or dob_derived is null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from data_warehouse.member_enrollment_monthly
	group by 4,
		3
	) a;


-----------------------------------
-----death_date
------------------------------------

--------death_date------mcrt mcrn optd----

insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'death_date' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select sum(case
				when death_date between '1800-01-01' and '2050-01-01'
					then 1
				end) as validvalues,
		coalesce(sum(case
					when death_date not between '1800-01-01' and '2050-01-01'
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from data_warehouse.member_enrollment_monthly
	where data_source in ('mcrn', 'mcrt', 'optd')
	group by 4,
		3
	) a;


--------death_date------mcrt mcrn optd----

insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'death_date' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'null is correct/valid value' as note
from (
	select sum(case
				when death_date is null
					then 1
				end) as validvalues,
		coalesce(sum(case
					when death_date is not null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from data_warehouse.member_enrollment_monthly
	where data_source in ('optz', 'mdcd', 'truv')
	group by 4,
		3
	) a;


-----------------------------------
-----plan_type
------------------------------------

--------plan_type------truv optz optd----
insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'plan_type' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'from reference_tables.ref_plan_type' as note
from (
	select sum(case
				when plan_type in ('ALL', 'EPO', 'GPO', 'HMO', 'IND', 'IPP', 'NONE', 'OTH', 'POS', 'PPO', 'SPN', 'UNK', 'BMM', 'CMP', 'EPO', 'HMO', 'POS', 'PPO', 'POS', 'CDHP', 'HDHP')
					then 1
				end) as validvalues,
		coalesce(sum(case
					when plan_type not in ('ALL', 'EPO', 'GPO', 'HMO', 'IND', 'IPP', 'NONE', 'OTH', 'POS', 'PPO', 'SPN', 'UNK', 'BMM', 'CMP', 'EPO', 'HMO', 'POS', 'PPO', 'POS', 'CDHP', 'HDHP')
						or plan_type is null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from data_warehouse.member_enrollment_monthly
	where data_source in ('truv', 'optz', 'optd')
	group by 4,
		3
	) a;
	

--------plan_type------mcrn mcrt---
insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'plan_type' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'from reference_tables.ref_medicare_entlmt_buyin' as note
from (
	select sum(case
				when plan_type in ('AB', 'B', 'A', 'AB', 'A', 'B')
					then 1
				end) as validvalues,
		coalesce(sum(case
					when plan_type not in ('AB', 'B', 'A', 'AB', 'A', 'B')
						or plan_type is null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from data_warehouse.member_enrollment_monthly
	where data_source in ('mcrn', 'mcrt')
	group by 4,
		3
	) a;

--------plan_type------mdcd----
insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'plan_type' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'null is valid plan_type for medicaid' as note
from (
	select sum(case
				when plan_type is null
					then 1
				end) as validvalues,
		coalesce(sum(case
					when plan_type is not null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from data_warehouse.member_enrollment_monthly
	where data_source in ('mdcd')
	group by 4,
		3
	) a;


-----------------------------------
-----bus_cd
------------------------------------




insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'bus_cd' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select sum(case
				when bus_cd in ('COM', 'MDCR', 'MCR', 'MCD')
					then 1
				end) as validvalues,
		coalesce(sum(case
					when bus_cd not in ('COM', 'MDCR', 'MCR', 'MCD')
						or bus_cd is null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from data_warehouse.member_enrollment_monthly
	--where data_source in ('mcrn','mcrt')
	group by 4,
		3
	) a;


-----------------------------------
-----employee_status
------------------------------------

-------------truven-----------------------
insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'employee_status' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select sum(case
				when employee_status between '1' and '9'
					then 1
				end) as validvalues,
		coalesce(sum(case
					when employee_status not between '1' and '9'
						or employee_status is null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from data_warehouse.member_enrollment_monthly
	where data_source = 'truv'
	group by 4,
		3
	) a;

------------------------------------
insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'employee_status' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'null is valid value' as note
from (
	select sum(case
				when employee_status is null
					then 1
				end) as validvalues,
		coalesce(sum(case
					when employee_status is not null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from data_warehouse.member_enrollment_monthly
	where data_source in ('mcrn', 'optd', 'mdcd', 'optz', 'mcrt')
	group by 4,
		3
	) a;


-----------------------------------
-----claim_created_flag
------------------------------------

	
	-------------------not created yet 


-----------------------------------
-----row_identifier
------------------------------------

	
insert into qa_reporting.monthly_enrollment_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'row_identifier' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when pg_typeof(row_identifier)::text like 'bigint'
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when pg_typeof(row_identifier)::text not like 'bigint'
                        or row_identifier is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from data_warehouse.member_enrollment_monthly
    group by 4,
        3
    ) a;	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	


-----------------------------------
-----rx_coverage
------------------------------------


insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'rx_coverage' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select sum(case
				when rx_coverage between 0 and 1
					then 1
				end) as validvalues,
		coalesce(sum(case
					when rx_coverage not between 0 and 1
						or rx_coverage is null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from data_warehouse.member_enrollment_monthly
	group by 4,
		3
	) a;


-----------------------------------
-----fiscal_year
------------------------------------


insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'fiscal_year' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select sum(case
				when fiscal_year between 2007 and 2020
					then 1
				end) as validvalues,
		coalesce(sum(case
					when fiscal_year not between 2007 and 2020
						or fiscal_year is null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from data_warehouse.member_enrollment_monthly
	group by 4,
		3
	) a;


-----------------------------------
-----race_cd
------------------------------------


insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'race_cd' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select sum(case
				when race_cd::int between 0 and 6
					then 1
				end) as validvalues,
		coalesce(sum(case
					when race_cd::int not between 0 and 6
						or race_cd is null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from data_warehouse.member_enrollment_monthly
	where data_source in ('mcrn', 'optd', 'mdcd', 'mcrt')
	group by 4,
		3
	) a;
	
	
------optz--------truv----------
insert into qa_reporting.monthly_enrollment_column_checks (
	test_var,
	validvalues,
	invalidvalues,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'race_cd' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'null is valid value' as note
from (
	select sum(case
				when race_cd is null
					then 1
				end) as validvalues,
		coalesce(sum(case
					when race_cd is not null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from data_warehouse.member_enrollment_monthly
	where data_source in ('optz', 'truv')
	group by 4,
		3
	) a;
	
	-- cleanup


drop table if exists dev.qa_temp_all_states;
drop table if exists dev.qa_temp_ids_src;