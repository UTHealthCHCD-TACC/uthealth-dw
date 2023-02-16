
/*
 *
 *
--------------------------------------------------------------------------------
--********************************************----------------------------------
--------   dw_staging.claim_diagColumn QA ------------------
--********************************************----------------------------------
--------------------------------------------------------------------------------
--- tu001 | 9/8/21 | script creation
*/

drop table if exists qa_reporting.claim_diag_column_checks ;

create table qa_reporting.claim_diag_column_checks 
  ( 
     test_var        UNKNOWN null, 
     valid_values     INT8 null, 
     invalid_values   INT8 null, 
     percent_invalid NUMERIC null, 
     pass_threshold  BOOL null, 
     "year"          INT2 null, 
     data_source     BPCHAR(4) null, 
     note            text null 
  )  ;

 

------------------------------------
--------data source---------
------------------------------------


insert into qa_reporting.claim_diag_column_checks (
    test_var,
    valid_values,
    invalid_values,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'data_source' as test_var,
	valid_values,
	invalid_values,
	invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
	((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select sum(case
				when data_source in ('mcrt', 'optz', 'mdcd', 'mcrn', 'truv', 'optd')
					then 1
				end) as valid_values,
		coalesce(sum(case
					when data_source not in ('mcrt', 'optz', 'mdcd', 'mcrn', 'truv', 'optd')
						then 1
					end), 0) as invalid_values,
		extract (year from from_date_of_service) as year,
		data_source
	from data_warehouse.claim_diag
	group by data_source,
		extract (year from from_date_of_service)
	) a;



------------------------------------
--------claim_sequence_number-------max is currently 667
------------------------------------

insert into qa_reporting.claim_diag_column_checks (
    test_var,
    valid_values,
    invalid_values,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'claim_sequence_number' as test_var,
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as notes
from (
    select sum(case
                when claim_sequence_number between 1 and 700
                    then 1
                end) as valid_values,
        coalesce(sum(case
                    when claim_sequence_number not between 1 and 700
                        or claim_sequence_number is null
                        then 1
                    end), 0) as invalid_values,
        extract (year from from_date_of_service) as year,
        data_source
    from data_warehouse.claim_diag
    group by data_source, extract (year from from_date_of_service)
    ) a;


---------------------------------   
-----uth_member_id---------------
---------------------------------
    

with ut_id_table
as (
    select a.uth_member_id,  extract (year from from_date_of_service) as year, a.data_source, b.uth_member_id as src_id
    from dw_staging.claim_diag a
    left join data_warehouse.dim_uth_member_id b on a.uth_member_id = b.uth_member_id
    )
insert into qa_reporting.claim_diag_column_checks (
    test_var,
    valid_values,
    invalid_values,
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
                when src_id is not null
                    then 1
                end) as valid_values,
        coalesce(sum(case
                    when src_id is null
                        then 1
                    end), 0) as invalid_values,
        year,
        data_source
    from ut_id_table
	group by data_source,
		year
    ) a;
   
   

---------------------------------   
-----uth_claim_id---------------
---------------------------------
    
with ut_id_table
as (
    select a.uth_claim_id,  extract (year from from_date_of_service) as year, a.data_source, b.uth_claim_id as src_id
    from dw_staging.claim_diag a
    left join data_warehouse.dim_uth_claim_id b on a.uth_claim_id = b.uth_claim_id
    )
insert into qa_reporting.claim_diag_column_checks (
    test_var,
    valid_values,
    invalid_values,
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
                when src_id is not null
                    then 1
                end) as valid_values,
        coalesce(sum(case
                    when src_id is null
                        then 1
                    end), 0) as invalid_values,
        year,
        data_source
    from ut_id_table
	group by data_source,
		year
    ) a;


------------------------------------
--------from_date_of_service--------
------------------------------------


insert into qa_reporting.claim_diag_column_checks (
    test_var,
    valid_values,
    invalid_values,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'from_date_of_service' as test_var,
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
     year,
    data_source,
    '' as note
from (
    select coalesce(sum(case
                when from_date_of_service between '2007-01-01' and current_date
                    then 1
                end),0) as valid_values,
        coalesce(sum(case
                    when from_date_of_service not between '2007-01-01' and current_date
                        or from_date_of_service is null
                        then 1
                    end), 0) as invalid_values,
        extract (year from from_date_of_service)  as year,
        data_source
    from data_warehouse.claim_diag
    group by data_source, extract (year from from_date_of_service)
    ) a;  
   


------------------------------------
--------diag code-------------------
------------------------------------

insert into qa_reporting.claim_diag_column_checks (
    test_var,
    valid_values,
    invalid_values,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'diag_cd' as test_var,
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
     year,
    data_source,
    '' as note
from (
    select sum(case
                when diag_cd ~ '^[[:alnum:]]{3,7}$'
                and diag_cd is not null
                    then 1
                end) as valid_values,
        coalesce(sum(case
                    when (diag_cd !~ '^[[:alnum:]]{3,7}$'
                    and diag_cd is not null)
                    or diag_cd is null
                        then 1
                    end), 0) as invalid_values,
        extract (year from from_date_of_service) as year,
        data_source
    from dw_staging.claim_diag
	group by data_source,
		extract (year from from_date_of_service)
    ) a;
   


------------------------------------
--------diag_position---------------
------------------------------------

insert into qa_reporting.claim_diag_column_checks (
    test_var,
    valid_values,
    invalid_values,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'diag_position' as test_var,
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
     year,
    data_source,
    '' as notes
from (
    select sum(case
                when diag_position between 1 and 25 
                or diag_position is null
                    then 1
                end) as valid_values,
        coalesce(sum(case
                    when diag_position not between 1 and 25
                        and diag_position is not null
                        then 1
                    end), 0) as invalid_values,
        extract (year from from_date_of_service) as year,
        data_source
    from data_warehouse.claim_diag
    group by data_source, extract (year from from_date_of_service)
    ) a;



 
------------------------------------
--------poa_src--------------------
------------------------------------

insert into qa_reporting.claim_diag_column_checks (
    test_var,
    valid_values,
    invalid_values,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'poa_src' as test_var,
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
     year,
    data_source,
    '' as notes
from (
    select sum(case
                when poa_src in ('0', '1','Y','N','U') or poa_src is null
                    then 1
                end) as valid_values,
        coalesce(sum(case
                    when poa_src not in ('0', '1','Y','N','U') and poa_src is not null
                        then 1
                    end), 0) as invalid_values,
        extract (year from from_date_of_service)  as year,
        data_source
    from data_warehouse.claim_diag
    group by data_source, extract (year from from_date_of_service)
    ) a;

   
   
------------------------------------\
-----icd_version--------------------
------------------------------------

insert into qa_reporting.claim_diag_column_checks (
    test_var,
    valid_values,
    invalid_values,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'icd_version' as test_var,
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as notes
from (
    select sum(case
                when icd_version = '10' or icd_version = '0'
                    then 1
                end) as valid_values,
        coalesce(sum(case
                    when icd_version != '10' and icd_version != '0' and icd_version is not null
                        or icd_version is null
                        then 1
                    end), 0) as invalid_values,
        extract (year from from_date_of_service) as year,
        data_source
    from dw_staging.claim_diag 
    group by data_source, extract (year from from_date_of_service) 
    ) a;
