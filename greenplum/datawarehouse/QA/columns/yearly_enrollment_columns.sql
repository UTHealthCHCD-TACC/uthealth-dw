/*
 *
 *
--------------------------------------------------------------------------------
--********************************************----------------------------------
--------   dw_staging.member_enrollment_yearly Column QA ------------------
--********************************************----------------------------------
--------------------------------------------------------------------------------

--- jw001 | 9/28/2021  | script creation
--- jw002 | 10/20/2021 | point all at staging

*/


drop table if exists qa_reporting.yearly_enrollment_column_checks ;

create table qa_reporting.yearly_enrollment_column_checks 
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


insert into qa_reporting.yearly_enrollment_column_checks (
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
	from dw_staging.member_enrollment_yearly
    group by data_source, year
	) a;


------------------------------------
--------year--------------
------------------------------------


insert into qa_reporting.yearly_enrollment_column_checks (
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
				when year between 2007 and 2021
					then 1
				end) as validvalues,
		coalesce(sum(case
					when year not between 2007 and 2021
						or year is null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from dw_staging.member_enrollment_yearly
    group by data_source, year
	) a;


	
------------------------------------
--------total_enrolled_months--------------
------------------------------------


insert into qa_reporting.yearly_enrollment_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'total_enrolled_months' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as notes
from (
    select sum(case
                when total_enrolled_months = enrolled_jan + enrolled_feb + enrolled_mar + enrolled_apr + enrolled_may + enrolled_jun +
enrolled_jul + enrolled_aug + enrolled_sep + enrolled_oct + enrolled_nov + enrolled_dec
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when total_enrolled_months != enrolled_jan + enrolled_feb + enrolled_mar + enrolled_apr + enrolled_may + enrolled_jun +
enrolled_jul + enrolled_aug + enrolled_sep + enrolled_oct + enrolled_nov + enrolled_dec
                    then 1
                end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.member_enrollment_yearly
    group by data_source, year
    ) a;	
	

------------------------------------
--------gender_cd--------------
------------------------------------


insert into qa_reporting.yearly_enrollment_column_checks (
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
    from dw_staging.member_enrollment_yearly
group by data_source, year
    ) a;
	



-----------------------------------
-----race_cd
------------------------------------


insert into qa_reporting.yearly_enrollment_column_checks (
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
                when (race_cd::int between 0 and 6 and race_cd is not null )
                or ( race_cd is null and data_source = 'optz')
                    then 1 
                end) as validvalues,
        coalesce(sum(case
                    when race_cd::int not between 0 and 6
                        and race_cd is not null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.member_enrollment_yearly
    group by data_source, year
    ) a;
    


-----------------------------------
-----age_derived
------------------------------------

insert into qa_reporting.yearly_enrollment_column_checks (
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
    from dw_staging.member_enrollment_yearly
    group by data_source, year
    ) a;







-----------------------------------
-----dob_derived
------------------------------------


insert into qa_reporting.yearly_enrollment_column_checks (
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
    from dw_staging.member_enrollment_yearly
    group by data_source, year
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
    select a.*, b.state as state_check
    from dw_staging.member_enrollment_yearly a
    left join dev.qa_temp_all_states b on a.state = b.state
    )
insert into qa_reporting.yearly_enrollment_column_checks (
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
                when state_check is not null or zip5 = '00000'
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when state_check is null and zip5 <> '00000'
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from state_table
group by data_source, year
    ) a;




-----------------------------------
-----zip5
------------------------------------


insert into qa_reporting.yearly_enrollment_column_checks (
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
                when ( zip5 ~ '^\d{5}$' and data_source in ('mcrn', 'mdcd', 'optz', 'mcrt'))
                or (zip5 is null and data_source in ('truv', 'optd'))
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when ( zip5 !~ '^\d{5}$' and data_source in ('mcrn', 'mdcd', 'optz', 'mcrt'))
                        or (zip5 is not null and data_source in ('truv', 'optd'))
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.member_enrollment_yearly
    group by data_source, year
    ) a;


-----------------------------------
-----zip3
------------------------------------

insert into qa_reporting.yearly_enrollment_column_checks (
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
                when ( zip3 ~ '^\d{3}$' and data_source in ('mcrn', 'mdcd', 'optz', 'mcrt','truv'))
                or (zip3 is null and data_source = 'optd')
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when ( zip3 !~ '^\d{3}$' and data_source in ('mcrn', 'mdcd', 'optz', 'mcrt','truv'))
                        or (zip3 is not null and data_source in ('optd'))
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.member_enrollment_yearly
    group by data_source, year
    ) a;




-----------------------------------
-----death_date
------------------------------------

insert into qa_reporting.yearly_enrollment_column_checks (
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
                when (death_date between '1800-01-01' and '2050-01-01' and data_source in ('mcrn', 'mcrt', 'optd'))
                or (death_date is null and data_source in ('optz', 'mdcd', 'truv'))
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when (death_date not between '1800-01-01' and '2050-01-01' and data_source in ('mcrn', 'mcrt', 'optd'))
                    or (death_date is not null and data_source in ('optz', 'mdcd', 'truv'))
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.member_enrollment_yearly
    group by data_source, year
    ) a;





-----------------------------------
-----plan_type
------------------------------------

insert into qa_reporting.yearly_enrollment_column_checks (
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
                when (plan_type in ('ALL', 'EPO', 'GPO', 'HMO', 'IND', 'IPP', 'NONE', 'OTH', 'POS', 'PPO', 'SPN', 'UNK', 'BMM', 'CMP', 'EPO', 'HMO', 'POS', 'PPO', 'POS', 'CDHP', 'HDHP')
                    and data_source in ('optz', 'optd'))
                    or ( plan_type in ('ALL', 'EPO', 'GPO', 'HMO', 'IND', 'IPP', 'NONE', 'OTH', 'POS', 'PPO', 'SPN', 'UNK', 'BMM', 'CMP', 'EPO', 'HMO', 'POS', 'PPO', 'POS', 'CDHP', 'HDHP')
                    and data_source in ('truv'))  
                    or (plan_type in ('AB', 'B', 'A', 'AB', 'A', 'B') 
                    and data_source in ('mcrt','mcrn'))
                    or (plan_type is null and data_source = 'mdcd')
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when (plan_type not in ('ALL', 'EPO', 'GPO', 'HMO', 'IND', 'IPP', 'NONE', 'OTH', 'POS', 'PPO', 'SPN', 'UNK', 'BMM', 'CMP', 'EPO', 'HMO', 'POS', 'PPO', 'POS', 'CDHP', 'HDHP')
                       and data_source in ('optz', 'optd'))
                    or ( plan_type not in ('ALL', 'EPO', 'GPO', 'HMO', 'IND', 'IPP', 'NONE', 'OTH', 'POS', 'PPO', 'SPN', 'UNK', 'BMM', 'CMP', 'EPO', 'HMO', 'POS', 'PPO', 'POS', 'CDHP', 'HDHP')
                    and data_source in ('truv'))
                    or (plan_type not in ('AB', 'B', 'A', 'AB', 'A', 'B') 
                    and data_source in ('mcrt','mcrn'))
                    or (plan_type is not null and data_source = 'mdcd')
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.member_enrollment_yearly
    group by data_source, year
    ) a;




-----------------------------------
-----bus_cd
------------------------------------
insert into qa_reporting.yearly_enrollment_column_checks (
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
				when bus_cd in ('COM', 'MDCR', 'MCR', 'MCD','MA','MS')
				or (bus_cd is null and data_source in ('mcrt','mcrn','mdcd'))
					then 1
				end) as validvalues,
		coalesce(sum(case
					when bus_cd not in ('COM', 'MDCR', 'MCR', 'MCD','MA','MS')
						and bus_cd is not null 
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from dw_staging.member_enrollment_yearly 
    group by data_source, year
	) a;













-----------------------------------
-----employee_status
------------------------------------

insert into qa_reporting.yearly_enrollment_column_checks (
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
                when (employee_status between '1' and '9' and data_source = 'truv')
                or (employee_status is null and data_source in ('mcrn', 'optd', 'mdcd', 'optz', 'mcrt'))
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when (employee_status not between '1' and '9'
                        or employee_status is null and data_source = 'truv')
                        or (employee_status is not null and data_source in ('mcrn', 'optd', 'mdcd', 'optz', 'mcrt'))
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.member_enrollment_yearly
    group by data_source, year
    ) a;



-----------------------------------
-----enrolled_jan
------------------------------------


insert into qa_reporting.yearly_enrollment_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'enrolled_jan' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when enrolled_jan between 0 and 1
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when enrolled_jan not between 0 and 1
                        or enrolled_jan is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.member_enrollment_yearly
    group by data_source, year
    ) a;

-----------------------------------
-----enrolled_feb
------------------------------------


insert into qa_reporting.yearly_enrollment_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'enrolled_feb' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when enrolled_feb between 0 and 1
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when enrolled_feb not between 0 and 1
                        or enrolled_feb is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.member_enrollment_yearly
    group by data_source, year
    ) a;

-----------------------------------
-----enrolled_mar
------------------------------------


insert into qa_reporting.yearly_enrollment_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'enrolled_mar' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when enrolled_mar between 0 and 1
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when enrolled_mar not between 0 and 1
                        or enrolled_mar is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.member_enrollment_yearly
    group by data_source, year
    ) a;


-----------------------------------
-----enrolled_apr
------------------------------------


insert into qa_reporting.yearly_enrollment_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'enrolled_apr' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when enrolled_apr between 0 and 1
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when enrolled_apr not between 0 and 1
                        or enrolled_apr is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.member_enrollment_yearly
    group by data_source, year
    ) a;

-----------------------------------
-----enrolled_may
------------------------------------


insert into qa_reporting.yearly_enrollment_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'enrolled_may' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when enrolled_may between 0 and 1
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when enrolled_may not between 0 and 1
                        or enrolled_may is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.member_enrollment_yearly
    group by data_source, year
    ) a;


-----------------------------------
-----enrolled_jun
------------------------------------


insert into qa_reporting.yearly_enrollment_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'enrolled_jun' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when enrolled_jun between 0 and 1
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when enrolled_jun not between 0 and 1
                        or enrolled_jun is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.member_enrollment_yearly
    group by data_source, year
    ) a;

-----------------------------------
-----enrolled_jul
------------------------------------


insert into qa_reporting.yearly_enrollment_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'enrolled_jul' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when enrolled_jul between 0 and 1
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when enrolled_jul not between 0 and 1
                        or enrolled_jul is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.member_enrollment_yearly
    group by data_source, year
    ) a;

-----------------------------------
-----enrolled_aug
------------------------------------


insert into qa_reporting.yearly_enrollment_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'enrolled_aug' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when enrolled_aug between 0 and 1
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when enrolled_aug not between 0 and 1
                        or enrolled_aug is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.member_enrollment_yearly
    group by data_source, year
    ) a;

-----------------------------------
-----enrolled_sep
------------------------------------


insert into qa_reporting.yearly_enrollment_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'enrolled_sep' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when enrolled_sep between 0 and 1
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when enrolled_sep not between 0 and 1
                        or enrolled_sep is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.member_enrollment_yearly
    group by data_source, year
    ) a;


-----------------------------------
-----enrolled_oct
------------------------------------


insert into qa_reporting.yearly_enrollment_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'enrolled_oct' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when enrolled_oct between 0 and 1
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when enrolled_oct not between 0 and 1
                        or enrolled_oct is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.member_enrollment_yearly
    group by data_source, year
    ) a;

-----------------------------------
-----enrolled_nov
------------------------------------


insert into qa_reporting.yearly_enrollment_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'enrolled_nov' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when enrolled_nov between 0 and 1
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when enrolled_nov not between 0 and 1
                        or enrolled_nov is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.member_enrollment_yearly
    group by data_source, year
    ) a;

-----------------------------------
-----enrolled_dec
------------------------------------


insert into qa_reporting.yearly_enrollment_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'enrolled_dec' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when enrolled_dec between 0 and 1
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when enrolled_dec not between 0 and 1
                        or enrolled_dec is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.member_enrollment_yearly
    group by data_source, year
    ) a;








	
---------------------------------	
-----uth_member_id---------------
---------------------------------
	
-------------------------check 1: in dim table	
with ut_id_table
as (
    select a.uth_member_id, 
    				a."year", 
    				a.data_source, 
    				b.uth_member_id as dim_id
    from dw_staging.member_enrollment_yearly a
    left join data_warehouse.dim_uth_member_id b on a.uth_member_id = b.uth_member_id
    )
insert into qa_reporting.yearly_enrollment_column_checks (
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
                when dim_id is not null
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when dim_id is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from ut_id_table
    group by data_source, year
    ) a;


------------check 2 in src table


/*drop table if exists dev.qa_temp_ids_src;

    select distinct patidsrc
    into dev.qa_temp_ids_src
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
        from uthealth/medicare_national.mbsf_abcd_summary
        
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
    
with ut_id_table
as (
    select a.uth_member_id, a."year", a.data_source, b.member_id_src as dim_id
    from dw_staging.member_enrollment_yearly a
    join data_warehouse.dim_uth_member_id b 
        on a.uth_member_id = b.uth_member_id
    left outer join dev.qa_temp_ids_src c 
        on b.member_id_src = c.patidsrc
    )    
insert into qa_reporting.yearly_enrollment_column_checks (
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
                when dim_id is not null
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when dim_id is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from ut_id_table
group by data_source, year
    ) a;




-----------------------------------
-----claim_created_flag
------------------------------------

	
	-------------------not created yet 


	
	
	
	


-----------------------------------
-----rx_coverage
------------------------------------


insert into qa_reporting.yearly_enrollment_column_checks (
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
	from dw_staging.member_enrollment_yearly
    group by data_source, year
	) a;


-----------------------------------
-----fiscal_year
------------------------------------


insert into qa_reporting.yearly_enrollment_column_checks (
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
				when fiscal_year between 2007 and 2021
					then 1
				end) as validvalues,
		coalesce(sum(case
					when fiscal_year not between 2007 and 2021
						or fiscal_year is null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from dw_staging.member_enrollment_yearly
    group by data_source, year
	) a;
