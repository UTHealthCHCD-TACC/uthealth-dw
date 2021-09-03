/*
 *
 *
--------------------------------------------------------------------------------
--********************************************----------------------------------
--------   data_warehouse.claim_detail Column QA ------------------
--********************************************----------------------------------
--------------------------------------------------------------------------------

--- jw001 | 8/16/21 | script creation

*/


drop table if exists dev.claim_detail_column_checks ;

create table dev.claim_detail_column_checks 
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


insert into dev.claim_detail_column_checks (
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
    from data_warehouse.claim_detail
    group by data_source, year
    ) a;


------------------------------------
--------year--------------
------------------------------------
-- jw right now year is extracted from date of service for medicaid
-- causes tables to be a bit crazy will look into later talk to lopita


insert into dev.claim_detail_column_checks (
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
    from data_warehouse.claim_detail
    group by data_source, year
    ) a;


---------------------------------   
-----uth_claim_id---------------
---------------------------------
    
-------------------------check 1: in dim table  

with ut_id_table
as (
    select a.uth_claim_id, a."year", a.data_source 
    from data_warehouse.claim_detail a
    left join data_warehouse.dim_uth_claim_id b on a.uth_claim_id = b.uth_claim_id
    )
insert into dev.claim_detail_column_checks (
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
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    'validate in data_warehouse.dim_uth_claim_id' as note
from (
    select sum(case
                when uth_claim_id is not null
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when uth_claim_id is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from ut_id_table
    group by data_source, year
    ) a;


------------------------------------
--------claim_sequence_number-------max is currently 667
------------------------------------

insert into dev.claim_detail_column_checks (
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
                when claim_sequence_number between 1 and 700
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when claim_sequence_number not between 1 and 700
                        or claim_sequence_number is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from data_warehouse.claim_detail
    group by data_source, year
    ) a;




---------------------------------   
-----uth_member_id---------------
---------------------------------
    
-------------------------check 1: in dim table  
with ut_id_table
as (
    select a.uth_member_id, a."year", a.data_source 
    from data_warehouse.claim_detail a
    left join data_warehouse.dim_uth_member_id b on a.uth_member_id = b.uth_member_id
    )
insert into dev.claim_detail_column_checks (
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
    group by data_source, year
    ) a;




    
------------------------------------
--------from_date_of_service--------------
------------------------------------


insert into dev.claim_detail_column_checks (
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
    from data_warehouse.claim_detail
    group by data_source, year
    ) a;

------------------------------------
--------to_date_of_service--------------
------------------------------------


insert into dev.claim_detail_column_checks (
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
    from data_warehouse.claim_detail
    group by data_source, year
    ) a;




------------------------------------
--------month_year_id--------------
------------------------------------


insert into dev.claim_detail_column_checks (
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
    from data_warehouse.claim_detail
    group by data_source, year
    ) a;



-----------------------------------
-----place_of_service
------------------------------------
--lopita question
    -- is this supposed to always be filled? 
    --

with pos_table
as (
    select a.uth_claim_id, a.data_source, a."year", b.place_of_treatment_cd 
    from data_warehouse.claim_detail a
    left join reference_tables.ref_place_of_service b on a.place_of_service = b.place_of_treatment_cd
    )
/*insert into dev.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )*/
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
                when place_of_treatment_cd is not null
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when place_of_treatment_cd is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from pos_table
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


insert into dev.claim_detail_column_checks (
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
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce(sum(case
                when admit_date between '2007-01-01' and current_date
                    then 1
                end),0) as validvalues,
        coalesce(sum(case
                    when admit_date not between '2007-01-01' and current_date
                        or admit_date is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from data_warehouse.claim_detail
    group by data_source, year
    ) a;





------------------------------------
--------discharge_date--------------
------------------------------------


insert into dev.claim_detail_column_checks (
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
                when discharge_date between '2007-01-01' and current_date
                    then 1
                end),0) as validvalues,
        coalesce(sum(case
                    when discharge_date not between '2007-01-01' and current_date
                        or discharge_date is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from data_warehouse.claim_detail
    group by data_source, year
    ) a;



-----------------------------------
-----cpt_hcpcs
------------------------------------




-----------------------------------
-----procedure_type
------------------------------------

------------------------------------
--proc_mod_1
------------------------------------

------------------------------------
--proc_mod_2
------------------------------------



------------------------------------
--revenue_cd
------------------------------------

------------------------------------
--charge_amount
------------------------------------


/*insert into dev.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )*/
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
    from data_warehouse.claim_detail
    group by data_source, year
    ) a;

----------------
--allowed_amount
----------------

/*insert into dev.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )*/
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
    from data_warehouse.claim_detail
    group by data_source, year
    ) a;


------------------------------------
--paid_amount
------------------------------------

/*insert into dev.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )*/
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
    from data_warehouse.claim_detail
    group by data_source, year
    ) a;













------------------------------------
--copay
------------------------------------




------------------------------------
--deductible
------------------------------------




------------------------------------
--coins
------------------------------------




------------------------------------
--cob
------------------------------------





------------------------------------
--bill_type_inst
------------------------------------

insert into dev.claim_detail_column_checks (
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
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from data_warehouse.claim_detail
    group by data_source, year
    ) a;


------------------------------------
--------bill_type_class---------
------------------------------------


insert into dev.claim_detail_column_checks (
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
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from data_warehouse.claim_detail
    group by data_source, year
    ) a;


------------------------------------
--bill_type_freq
------------------------------------
-- others

/*insert into dev.claim_detail_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )*/
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
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from data_warehouse.claim_detail
    group by data_source, year
    ) a;








--units
--drg_cd
--claim_id_src
---member_id_src
--table_id_src
--claim_sequence_number_src
--cob_type
--fiscal_year
--cost_factor_year
--discharge_status














