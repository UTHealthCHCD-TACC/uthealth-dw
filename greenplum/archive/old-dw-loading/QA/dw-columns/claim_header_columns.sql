/*
 *
 *
--------------------------------------------------------------------------------
--********************************************----------------------------------
--------   dw_staging.claim_header Column QA ------------------
--********************************************----------------------------------
--------------------------------------------------------------------------------
--- tu | 9/1/21 | script creation
-- jw | 9/9/21 | edit 1
*/

drop table if exists qa_reporting.claim_header_column_checks ;

create table qa_reporting.claim_header_column_checks 
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


insert into qa_reporting.claim_header_column_checks (
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
	from dw_staging.claim_header
	group by data_source,
		year
	) a;


------------------------------------
--------year--------------
------------------------------------

insert into qa_reporting.claim_header_column_checks (
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
				when year between 2007 and 2021
					then 1
				end) as valid_values,
		coalesce(sum(case
					when year not between 2007 and 2021
						or year is null
						then 1
					end), 0) as invalid_values,
		year,
		data_source
	from dw_staging.claim_header
	group by data_source,
		year
	) a;
	


---------------------------------
-----uth_claim_id----------------
---------------------------------


with ut_claim_id_table
as (
    select a.uth_claim_id, a."year", a.data_source, b.uth_claim_id as dim_id
    from dw_staging.claim_header a
    left join data_warehouse.dim_uth_claim_id b on a.uth_claim_id = b.uth_claim_id
    )
insert into qa_reporting.claim_header_column_checks (
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

   
---------------------------------	
-----uth_member_id---------------
---------------------------------
	
   
with ut_id_table
as (
    select a.uth_member_id, a."year", a.data_source, b.uth_member_id as src_id
    from dw_staging.claim_header a
    left join data_warehouse.dim_uth_member_id b on a.uth_member_id = b.uth_member_id
    )
insert into qa_reporting.claim_header_column_checks (
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
   
   
   
------------------------------------
--------from_date_of_service--------
------------------------------------

   
insert into qa_reporting.claim_header_column_checks (
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
    from dw_staging.claim_header
    group by data_source, year
    ) a;

   
   
------------------------------------
--------to_date_of_service--------------
------------------------------------


insert into qa_reporting.claim_header_column_checks (
    test_var,
    valid_values,
    invalid_values,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'to_date_of_service' as test_var,
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce(sum(case
                when to_date_of_service between '2007-01-01' and current_date
                    then 1
                end),0) as valid_values,
        coalesce(sum(case
                    when to_date_of_service not between '2007-01-01' and current_date
                        or to_date_of_service is null
                        then 1
                    end), 0) as invalid_values,
        year,
        data_source
    from dw_staging.claim_header
    group by data_source, year
    ) a;   
   
---truven missing to_date_of_service in insert statement
   
   -----------------------------------
-----claim_type--------------------
-----------------------------------

insert into qa_reporting.claim_header_column_checks (
    test_var,
    valid_values,
    invalid_values,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'claim_type' as test_var,
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when claim_type ~ '^(F|P)$' then 1
                end) as valid_values,
        coalesce(sum(case
                    when claim_type !~ '^(F|P)$' or claim_type is null
                        then 1
                    end), 0) as invalid_values,
        year,
        data_source
    from dw_staging.claim_header 
    group by data_source, year
    ) a;
   
   
---------------------------------	
-----uth_admission_id------------
---------------------------------
   
--if uth_admission does exist check	
   
with ut_admission_id_table
as (
    select b.uth_admission_id, a."year", a.data_source, b.uth_admission_id as dim_admit
    from dw_staging.claim_header a
    left join data_warehouse.dim_uth_admission_id b on a.uth_admission_id = b.uth_admission_id 
    where a.uth_admission_id is not null
    )
insert into qa_reporting.claim_header_column_checks (
    test_var,
    valid_values,
    invalid_values,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'uth_admission_id' as test_var,
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    'validate in data_warehouse.dim_uth_admission_id' as note
from (
    select sum(case
                when dim_admit is not null
                    then 1
                end) as valid_values,
        coalesce(sum(case
                    when dim_admit is null
                        then 1
                    end), 0) as invalid_values,
        year,
        data_source
    from ut_admission_id_table
       group by data_source, year
    ) a;   
   
   
   
------------------------------------
--total_charge_amount
------------------------------------


insert into qa_reporting.claim_header_column_checks (
    test_var,
    valid_values,
    invalid_values,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'total_charge_amount' as test_var,
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce(sum(case
                when total_charge_amount < 1000000 or total_charge_amount is null then 1 end
                ),0)  as valid_values,
        coalesce(sum(case
                    when total_charge_amount > 1000000
                        then 1
                    end), 0)  as invalid_values,
        year,
        data_source
    from dw_staging.claim_header
    group by data_source, year
    ) a;
   
   
------------------------------------
--total_allowed_amount
------------------------------------


insert into qa_reporting.claim_header_column_checks (
    test_var,
    valid_values,
    invalid_values,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'total_allowed_amount' as test_var,
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce(sum(case
                when total_allowed_amount < 1000000 or total_allowed_amount is null then 1 end
                ),0)  as valid_values,
        coalesce(sum(case
                    when total_allowed_amount > 1000000
                        then 1
                    end), 0)  as invalid_values,
        year,
        data_source
    from dw_staging.claim_header
    group by data_source, year
    ) a;   
   

------------------------------------
--total_paid_amount
------------------------------------


insert into qa_reporting.claim_header_column_checks (
    test_var,
    valid_values,
    invalid_values,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'total_paid_amount' as test_var,
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce(sum(case
                when total_paid_amount < 1000000 or total_paid_amount is null then 1 end
                ),0)  as valid_values,
        coalesce(sum(case
                    when total_paid_amount > 1000000 and total_paid_amount is not null 
                        then 1
                    end), 0)  as invalid_values,
        year,
        data_source
    from dw_staging.claim_header
    group by data_source, year
    ) a;      

-----------------------------------
-----fiscal_year
------------------------------------


insert into qa_reporting.claim_header_column_checks (
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
				when fiscal_year between 2007 and 2021
					then 1
				end) as valid_values,
		coalesce(sum(case
					when fiscal_year not between 2007 and 2021
						or fiscal_year is null
						then 1
					end), 0) as invalid_values,
		year,
		data_source
	from dw_staging.claim_header 
    group by data_source, year
	) a;



------------------------------------
--------cost factor year--------------
------------------------------------



insert into qa_reporting.claim_header_column_checks (
	test_var,
	valid_values,
	invalid_values,
	percent_invalid,
	pass_threshold,
	"year",
	data_source,
	note
	)
select 'cost_factor_year' as test_var,
	valid_values,
	invalid_values,
	invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
	((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as notes
from (
	select sum(case
				when (cost_factor_year between 2007 and 2021 and cost_factor_year is not null)
				or cost_factor_year is null
					then 1
				end) as valid_values,
		coalesce(sum(case
					when (cost_factor_year not between 2007 and 2021
						and cost_factor_year is not null)
						then 1
					end), 0) as invalid_values,
		year,
		data_source
	from dw_staging.claim_header
    group by data_source, year
	) a;
	
/*
-----------------------------------
-----bill_provider
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
                    when place_of_service !~ '^\d{1,2}$'
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.claim_detail 
    group by data_source, year
    ) a;
