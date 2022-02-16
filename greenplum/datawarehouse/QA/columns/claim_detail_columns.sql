
/*
 *
 *
--------------------------------------------------------------------------------
--********************************************----------------------------------
--------   dw_staging.claim_detail Column QA ------------------
--********************************************----------------------------------
--------------------------------------------------------------------------------

--- jw001 | 8/16/21  | script creation
--- jw002 | 10/20/21 | point at dw_staging

*/


drop table if exists qa_reporting.claim_detail_column_checks ;

create table qa_reporting.claim_detail_column_checks 
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


insert into qa_reporting.claim_detail_column_checks (
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
    from dw_staging.claim_detail
    group by data_source, year
    ) a;




------------------------------------
--------year--------------
------------------------------------


insert into qa_reporting.claim_detail_column_checks (
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
    from dw_staging.claim_detail
    group by data_source, year
    ) a;


---------------------------------   
-----uth_claim_id---------------
---------------------------------
    
with ut_claim_id_table
as (
    select a.uth_claim_id, a."year", a.data_source, b.uth_claim_id as dim_id
    from dw_staging.claim_detail a
    left join data_warehouse.dim_uth_claim_id b on a.uth_claim_id = b.uth_claim_id
    )
insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'uth_claim_id' as test_var,
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    'validate in data_warehouse.dim_uth_claim_id' as note
from (
    select sum(case
                when dim_id is not null
                    then 1
                end) as valid_values,
        coalesce(sum(case
                    when dim_id is null
                        then 1
                    end), 0) as invalid_values,
        year,
        data_source
    from ut_claim_id_table
	group by data_source,
		year
    ) a;


------------------------------------
--------claim_sequence_number-------
------------------------------------

insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'claim_sequence_number' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as notes
from (
    select sum(case
                when claim_sequence_number between 0 and 700
                or claim_sequence_number is null
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when claim_sequence_number not between 0 and 1000
                        and claim_sequence_number is not null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.claim_detail
    group by data_source, year
    ) a;




---------------------------------   
-----uth_member_id---------------
---------------------------------
    
with ut_id_table
as (
    select a.uth_member_id, a."year", a.data_source, b.uth_member_id as dim_id
    from dw_staging.claim_detail a
    left join data_warehouse.dim_uth_member_id b on a.uth_member_id = b.uth_member_id
    )
insert into qa_reporting.claim_detail_column_checks (
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
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    'validate in data_warehouse.dim_uth_member_id' as note
from (
    select sum(case
                when dim_id is not null
                    then 1
                end) as valid_values,
        coalesce(sum(case
                    when dim_id is null
                        then 1
                    end), 0) as invalid_values,
        year,
        data_source
    from ut_id_table
	group by data_source,
		year
    ) a;



    
------------------------------------
--------from_date_of_service--------------
------------------------------------


insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'from_date_of_service' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce(sum(case
                when from_date_of_service between '2007-01-01' and current_date
                    then 1
                end),0) as validvalues,
        coalesce(sum(case
                    when from_date_of_service not between '2007-01-01' and current_date
                        or from_date_of_service is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.claim_detail
    group by data_source, year
    ) a;

------------------------------------
--------to_date_of_service--------------
------------------------------------


insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'to_date_of_service' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce(sum(case
                when to_date_of_service between '2007-01-01' and current_date
                    then 1
                end),0) as validvalues,
        coalesce(sum(case
                    when to_date_of_service not between '2007-01-01' and current_date
                        or to_date_of_service is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.claim_detail
    group by data_source, year
    ) a;




------------------------------------
--------month_year_id--------------
------------------------------------


insert into qa_reporting.claim_detail_column_checks (
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
    select coalesce (sum(case
                when month_year_id between 200701 and 202112
                    then 1
                end),0) as validvalues,
        coalesce(sum(case
                    when month_year_id not between 200701 and 202112
                        or month_year_id is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.claim_detail
    group by data_source, year
    ) a;



-----------------------------------
-----place_of_service
------------------------------------
   
insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'place_of_service' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select  coalesce(sum(case
                when place_of_service ~ '^\d{1,2}$'
                    then 1
                end),0) as validvalues,
        coalesce(sum(case
                    when place_of_service !~ '^\d{1,2}$' and place_of_service is not null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.claim_detail 
    group by data_source, year
    ) a;

-----------------------------------
-----network_ind
------------------------------------


-----------------------------------
-----network_paid_ind
------------------------------------




------------------------------------
--------admit_date--------------
------------------------------------


insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'admit_date' as test_var,
    validvalues,
    invalidvalues,
    coalesce ((invalidvalues / (validvalues + invalidvalues)::numeric),0) as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    'FYI medicaid values for admit will include enourmous amount of crazy dates' as note
from (
    select coalesce(sum(case
                when (admit_date between '2007-01-01' and current_date
                and admit_date is not null) or admit_date is null 
                    then 1
                end),0) as validvalues,
        coalesce (sum(case
                    when admit_date not between '2007-01-01' and current_date
                    and admit_date is not null
                        then 1
                    end),0) as invalidvalues,
        year,
        data_source
    from dw_staging.claim_detail
    group by data_source, year
    ) a;


------------------------------------
--------discharge_date--------------
------------------------------------

--delete from qa_reporting.claim_detail_column_checks where test_var = 'discharge_date';
insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'discharge_date' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce(sum(case
                when (discharge_date between '2007-01-01' and current_date
                and discharge_date is not null) or discharge_date is null
                    then 1
                end),0) as validvalues,
        coalesce(sum(case
                    when discharge_date not between '2007-01-01' and current_date
                        and discharge_date is not null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.claim_detail
    group by data_source, year
    ) a;

select count(*) from dw_staging.claim_detail where admit_date is not null; 

-----------------------------------
-----cpt_hcpcs
------------------------------------

insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'cpt_hcpcs_cd' as test_var,
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when cpt_hcpcs_cd ~ '^[[:alnum:]]{5,7}$'
                and cpt_hcpcs_cd is not null
                    then 1
                end) as valid_values,
        coalesce(sum(case
                    when (cpt_hcpcs_cd !~ '^[[:alnum:]]{5,7}$'
                    and cpt_hcpcs_cd is not null)
                        then 1
                    end), 0) as invalid_values,
        year,
        data_source
    from dw_staging.claim_detail 
	group by data_source,
		year
    ) a;




-----------------------------------
-----procedure_type
------------------------------------
   
insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'procedure_type' as test_var,
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when procedure_type in ('CPT','HCPCS')
                and procedure_type is not null
                    then 1
                end) as valid_values,
        coalesce(sum(case
                    when procedure_type not in ('CPT','HCPCS')
                    and procedure_type is not null            
                        then 1
                    end), 0) as invalid_values,
        year,
        data_source
    from dw_staging.claim_detail 
	group by data_source,
		year
    ) a; 

------------------------------------
--proc_mod_1
------------------------------------
 insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'proc_mod_1' as test_var,
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when proc_mod_1 ~ '^[[:alnum:]]{2}$'
                and proc_mod_1 is not null
                or proc_mod_1 in ('',' ')
                    then 1
                end) as valid_values,
        coalesce(sum(case
                    when (proc_mod_1 !~ '^[[:alnum:]]{2}$'
                    and proc_mod_1 is not null
                    and proc_mod_1 not in ('',' '))
                        then 1
                    end), 0) as invalid_values,
        year,
        data_source
    from dw_staging.claim_detail 
	group by data_source,
		year
    ) a;  
   
   
   
------------------------------------
--proc_mod_2
------------------------------------

 insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'proc_mod_2' as test_var,
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when proc_mod_2 ~ '^[[:alnum:]]{2}$'
                and proc_mod_2 is not null
                or proc_mod_2 is null
                or proc_mod_2 in ('', ' ')
                    then 1
                end) as valid_values,
        coalesce(sum(case
                    when (proc_mod_2 !~ '^[[:alnum:]]{2}$'
                    and proc_mod_2 is not null and proc_mod_2 not in ('',' '))
                        then 1
                    end), 0) as invalid_values,
        year,
        data_source
    from dw_staging.claim_detail 
	group by data_source,
		year
    ) a;  

------------------------------------
--revenue_cd
------------------------------------



insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'revenue_cd' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when ( revenue_cd ~ '^\d{4}$' 
                    and revenue_cd is not null ) or revenue_cd is null
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when revenue_cd !~ '^\d{4}$' 
                    and revenue_cd is not null 
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.claim_detail 
    group by data_source, year
    ) a;   
   
------------------------------------
--charge_amount
------------------------------------


insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'charge_amount' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce(sum(case
                when charge_amount < 1000000 or charge_amount is null then 1 end
                ),0)  as validvalues,
        coalesce(sum(case
                    when charge_amount > 1000000
                        then 1
                    end), 0)  as invalidvalues,
        year,
        data_source
    from dw_staging.claim_detail
    group by data_source, year
    ) a;

----------------
--allowed_amount
----------------

insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'allowed_amount' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce(sum(case
                when allowed_amount < 1000000 or allowed_amount is null then 1 end
                ),0)  as validvalues,
        coalesce(sum(case
                    when allowed_amount > 1000000
                        then 1
                    end), 0)  as invalidvalues,
        year,
        data_source
    from dw_staging.claim_detail
    group by data_source, year
    ) a;


------------------------------------
--paid_amount
------------------------------------

insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'paid_amount' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce(sum(case
                when paid_amount < 1000000 or paid_amount is null then 1 end
                ),0)  as validvalues,
        coalesce(sum(case
                    when paid_amount > 1000000
                        then 1
                    end), 0)  as invalidvalues,
        year,
        data_source
    from dw_staging.claim_detail
    group by data_source, year
    ) a;













------------------------------------
--copay
------------------------------------

insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'copay' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce(sum(case
                when copay < 50000 or copay is null then 1 end
                ),0)  as validvalues,
        coalesce(sum(case
                    when copay > 50000
                        then 1
                    end), 0)  as invalidvalues,
        year,
        data_source
    from dw_staging.claim_detail
    group by data_source, year
    ) a;


------------------------------------
--deductible
------------------------------------
--- could goto source values and see what those highest ones are 
   
insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'deductible' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce(sum(case
                when deductible < 50000 or deductible is null then 1 end
                ),0)  as validvalues,
        coalesce(sum(case
                    when deductible > 50000
                        then 1
                    end), 0)  as invalidvalues,
        year,
        data_source
    from dw_staging.claim_detail
    group by data_source, year
    ) a;


------------------------------------
--coins
------------------------------------

insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'coins' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce(sum(case
                when coins < 50000 or coins is null then 1 end
                ),0)  as validvalues,
        coalesce(sum(case
                    when coins > 50000
                        then 1
                    end), 0)  as invalidvalues,
        year,
        data_source
    from dw_staging.claim_detail
    group by data_source, year
    ) a;


------------------------------------
--cob
------------------------------------

insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'cob' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce(sum(case
                when cob < 50000 or cob is null then 1 end
                ),0)  as validvalues,
        coalesce(sum(case
                    when cob > 50000
                        then 1
                    end), 0)  as invalidvalues,
        year,
        data_source
    from dw_staging.claim_detail
    group by data_source, year
    ) a;


------------------------------------
--bill_type_inst
------------------------------------

insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'bill_type_inst' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when bill_type_inst ~ '^\d{1}$'
                    or bill_type_inst is null then 1
                end) as validvalues,
        coalesce(sum(case
                    when bill_type_inst !~ '^\d{1}$'
                    and bill_type_inst is not null
                    and bill_type_inst not in ('', ' ')
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.claim_detail
    group by data_source, year
    ) a;


------------------------------------
--------bill_type_class---------
------------------------------------


insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'bill_type_class' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when bill_type_class ~ '^\d{1}$'
                    or bill_type_class is null then 1
                end) as validvalues,
        coalesce(sum(case
                    when bill_type_class !~ '^\d{1}$' 
                    and bill_type_class is not null
                    and bill_type_class  not in ('', ' ')
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.claim_detail
    group by data_source, year
    ) a;



------------------------------------
--bill_type_freq
------------------------------------

-- others

insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'bill_type_freq' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when bill_type_freq ~ '^[a-zA-Z0-9]{1}$'
                    or bill_type_freq is null then 1
                end) as validvalues,
        coalesce(sum(case
                    when bill_type_freq !~ '^[a-zA-Z0-9]{1}$'
                    and bill_type_freq is not null
                    and bill_type_freq  not in ('', ' ')
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.claim_detail
    group by data_source, year
    ) a;







------------------------------------   
--units 
------------------------------------   


--for optum we want to use alt units



------------------------------------   
--drg_cd 
------------------------------------



 insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'drg_cd' as test_var,
    valid_values,
    coalesce(invalid_values,0),
    coalesce(invalid_values / (valid_values + invalid_values)::numeric,0) as percent_invalid,
    (coalesce((invalid_values / (valid_values + invalid_values)::numeric),0) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce(sum(case
                when drg_cd ~ '^[[:alnum:]]{3,4}$'
                and drg_cd is not null
                    then 1
                end),0) as valid_values,
        sum(case
                    when (drg_cd !~ '^[[:alnum:]]{3,4}$'
                    and drg_cd is not null
                    and drg_cd not in ('', ' '))
                        then 1
                    end) as invalid_values,
        year,
        data_source
    from dw_staging.claim_detail 
	group by data_source,
		year
    ) a;  

------------------------------------
--table_id_src
------------------------------------   

insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'table_id_src' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    "year",
    data_source,
    '' as note
from (
    select sum(case
                when table_id_src in (
'dme_claims', 'inpatient_revenue_center', 'hospice_base_claims', 'outpatient_revenue_center', 
'hha_revenue_center', 'snf_base_claims',  'snf_revenue_center', 'bcarrier_claims',  'bcarrier_line',
'hospice_base_claims', 'hospice_revenue_center','enc_det', 'clm_detail', 
'medical','ccaeo', 'mdcro', 'mdcrs', 'ccaes') or table_id_src is null
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when table_id_src not in (
'dme_claims', 'inpatient_revenue_center', 'hospice_base_claims', 'outpatient_revenue_center', 
'hha_revenue_center', 'snf_base_claims',  'snf_revenue_center', 'bcarrier_claims',  'bcarrier_line',
'hospice_base_claims', 'hospice_revenue_center','enc_det', 'clm_detail', 
'medical','ccaeo', 'mdcro', 'mdcrs', 'ccaes') and table_id_src is not null 
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.claim_detail
    group by data_source , year
    ) a;

   delete from qa_reporting.claim_detail_column_checks where test_var = 'table_id_src';
   
   
   
   
------------------------------------   
--claim_sequence_number_src
------------------------------------   

-------change to check for numbers 
   
insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'claim_sequence_number_src' as test_var,
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as notes
from (
    select sum(case
                when claim_sequence_number_src ~ '^[[:alnum:]]*$'
                and claim_sequence_number_src is not null
                    then 1
                end) as valid_values,
        coalesce(sum(case
                    when (claim_sequence_number_src  !~ '^[[:alnum:]]*$'
                        and claim_sequence_number_src is not null)
                        or claim_sequence_number_src is null
                        then 1
                    end), 0) as invalid_values,
        "year",
        data_source
    from dw_staging.claim_detail 
    group by data_source, year
    ) a;   
   
------------------------------------   
--cob_type
------------------------------------
/*
insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'cob_type' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when cob in ('C', 'A', 'N', 'Y', 'I') or cob_type is null
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when cob not in ('C', 'A', 'N', 'Y', 'I')
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.claim_detail
    group by data_source , year
    ) a;
   
   */


-----------------------------------
-----fiscal_year
------------------------------------

insert into qa_reporting.claim_detail_column_checks (
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
	select coalesce (sum(case
				when fiscal_year between 2007 and 2021 or fiscal_year is null
					then 1
				end),0) as validvalues,
		coalesce(sum(case
					when fiscal_year not between 2007 and 2021
						and fiscal_year is not null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from dw_staging.claim_detail 
	group by data_source,
		year
	) a;
   
  
------------------------------------   
--cost_factor_year
------------------------------------

delete from qa_reporting.claim_detail_column_checks where test_var = 'cost_factor_year';
insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'cost_factor_year' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select sum(case
				when (cost_factor_year between 2007 and 2021 and cost_factor_year is not null) or cost_factor_year is null
					then 1
				end) as validvalues,
		coalesce(sum(case
					when cost_factor_year not between 2007 and 2021
						and cost_factor_year is not null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from dw_staging.claim_detail 
	group by data_source,
		year
	) a;   
   
------------------------------------
-- discharge_status
------------------------------------

insert into qa_reporting.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'discharge_status' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when (discharge_status ~ '^\d{2}$'
                    and discharge_status is not null) or discharge_status is null then 1 
                end) as validvalues,
        coalesce(sum(case
                    when discharge_status !~ '^\d{2}$' 
                    and discharge_status is not null
                    and discharge_status not in ('', ' ')
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.claim_detail
    group by data_source, year
    ) a;

