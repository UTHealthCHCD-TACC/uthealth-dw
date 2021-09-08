/*
 *
 *
--------------------------------------------------------------------------------
--********************************************----------------------------------
--------   data_warehouse.claim_diag Column QA ------------------
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
        year,
        data_source
    from data_warehouse.claim_diag 
    group by data_source, year
    ) a;


------------------------------------
--------year--------------
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
select 'year' as test_var,
	valid_values,
	invalid_values,
	invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
	((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as notes
from (
	select sum(case
				when year between 2007 and 2020
					then 1
				end) as valid_values,
		coalesce(sum(case
					when year not between 2007 and 2020
						or year is null
						then 1
					end), 0) as invalid_values,
		year,
		data_source
	from data_warehouse.claim_diag 
	group by 4,
		3
	) a;


---------------------------------   
-----uth_claim_id---------------
--------------------------------- 

with ut_id_table
as (
    select a.uth_claim_id, a."year", a.data_source 
    from data_warehouse.claim_diag a
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
                when uth_claim_id is not null
                    then 1
                end) as valid_values,
        coalesce(sum(case
                    when uth_claim_id is null
                        then 1
                    end), 0) as invalid_values,
        year,
        data_source
    from ut_id_table
    group by data_source, year
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
        year,
        data_source
    from data_warehouse.claim_diag
    group by data_source, year
    ) a;




---------------------------------   
-----uth_member_id---------------
---------------------------------
    
with ut_id_table
as (
    select a.uth_member_id, a."year", a.data_source 
    from data_warehouse.claim_detail a
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
                when uth_member_id is not null
                    then 1
                end) as valid_values,
        coalesce(sum(case
                    when uth_member_id is null
                        then 1
                    end), 0) as invalid_values,
        year,
        data_source
    from ut_id_table
    group by data_source, year
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
        year,
        data_source
    from data_warehouse.claim_diag
    group by data_source, year
    ) a;  
   
select distinct diag_cd from data_warehouse.claim_diag;

------------------------------------
--------diag code-------------------
------------------------------------

-- Need to figure out how to deal with both ICD 9 and ICD10

-- Official source ICD 10: https://www.cms.gov/medicare/icd-10/2021-icd-10-cm
-- Official source ICD 9: https://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/codes


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
                    then 1
                end) as valid_values,
        coalesce(sum(case
                    when diag_position not between 1 and 25
                        or diag_position is null
                        then 1
                    end), 0) as invalid_values,
        year,
        data_source
    from data_warehouse.claim_diag
    group by data_source, year
    ) a;

   
------------------------------------
--------icd_type--------------------
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
select 'icd_type' as test_var,
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as notes
from (
    select sum(case
                when icd_type = '10' or icd_type = '9'
                    then 1
                end) as valid_values,
        coalesce(sum(case
                    when icd_type != '10' and icd_type != '9'
                        or icd_type is null
                        then 1
                    end), 0) as invalid_values,
        year,
        data_source
    from data_warehouse.claim_diag
    group by data_source, year
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
                when icd_type = '0' or icd_type = '1'
                    then 1
                end) as valid_values,
        coalesce(sum(case
                    when icd_type != '0' and icd_type != '1'
                        or icd_type is null
                        then 1
                    end), 0) as invalid_values,
        year,
        data_source
    from data_warehouse.claim_diag
    group by data_source, year
    ) a;
   
-----------------------------------
-----fiscal_year------------------
-----------------------------------

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
select 'fiscal_year' as test_var,
	valid_values,
	invalid_values,
	invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
	((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select sum(case
				when fiscal_year between 2007 and 2020
					then 1
				end) as valid_values,
		coalesce(sum(case
					when fiscal_year not between 2007 and 2020
						or fiscal_year is null
						then 1
					end), 0) as invalid_values,
		year,
		data_source
	from data_warehouse.claim_diag 
	group by 4,
		3
	) a;

   