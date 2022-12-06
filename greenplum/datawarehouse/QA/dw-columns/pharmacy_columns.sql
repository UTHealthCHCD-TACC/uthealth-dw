/*
 *
 *
--------------------------------------------------------------------------------
--********************************************----------------------------------
--------   dw_staging.pharmacy_claims Column QA ------------------
--********************************************----------------------------------
--------------------------------------------------------------------------------

--- jw001 | 8/16/21  | script creation
--- jw002 | 10/20/21 | point at dw_staging

*/


drop table if exists qa_reporting.pharmacy_column_checks ;

create table qa_reporting.pharmacy_column_checks 
  ( 
     test_var        text null, 
     validvalues     INT8 null, 
     invalidvalues   INT8 null, 
     percent_invalid NUMERIC null, 
     pass_threshold  BOOL null, 
     "year"          INT2 null, 
     data_source     text null, 
     note            text null 
  )  ;
  
 
 ----
 
------------------------------------
--------data source---------
------------------------------------


insert into qa_reporting.pharmacy_column_checks (
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
    from dw_staging.pharmacy_claims 
    group by data_source, year
    ) a;

------------------------------------
--------year--------------
------------------------------------


insert into qa_reporting.pharmacy_column_checks (
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
                when year between 2007 and extract( year from current_date)::int
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when year not between 2007 and extract( year from current_date)::int
                        or year is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.pharmacy_claims
    group by data_source, year
    ) a;

---------------------------------   
-----uth_claim_id---------------
---------------------------------
    
with ut_claim_id_table
as (
    select a.uth_rx_claim_id, a."year", 
    	   a.data_source, b.uth_rx_claim_id as dim_id
    from dw_staging.pharmacy_claims a
    left join data_warehouse.dim_uth_rx_claim_id b on a.uth_rx_claim_id = b.uth_rx_claim_id
    )
insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source
    )
select 'uth_rx_claim_id' as test_var,
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
    year,
    data_source
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
    select a.uth_member_id, a."year", a.data_source, b.uth_member_id as dim_id
    from dw_staging.pharmacy_claims  a
    left join data_warehouse.dim_uth_member_id b on a.uth_member_id = b.uth_member_id
    )
insert into qa_reporting.pharmacy_column_checks (
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

   ------------------------------
   ----------fill_date
   ------------------------------
   
   insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'fill_date' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce(sum(case
                when fill_date between '2007-01-01' and current_date
                    then 1
                end),0) as validvalues,
        coalesce(sum(case
                    when fill_date not between '2007-01-01' and current_date
                        or fill_date is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.pharmacy_claims 
    group by data_source, year
    ) a;
    
   
   
-----------------------------------
-----ndc
------------------------------------
   
insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'ndc' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select  coalesce(sum(case
                when ndc ~ '^\d{11}$'
                    then 1
                end),0) as validvalues,
        coalesce(sum(case
                    when ndc !~ '^\d{11}$' and ndc is not null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.pharmacy_claims  
    group by data_source, year
    ) a;
    
   
   -----------------------------------
-----script_id
------------------------------------
   
insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'script_id' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select  coalesce(sum(case
                when script_id ~ '^[[:alnum:]]{0,12}$' 
                  or script_id is null 
                    then 1
                end),0) as validvalues,
        coalesce(sum(case
                    when script_id !~ '^[[:alnum:]]{0,12}$'
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.pharmacy_claims  
    group by data_source, year
    ) a;
    
   -----------------------------------
-----refill_count
------------------------------------
   
insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'refill_count' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select  coalesce(sum(case
                when refill_count between 0 and 999
                    then 1
                end),0) as validvalues,
        coalesce(sum(case
                    when refill_count between 0 and 999
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.pharmacy_claims  
    group by data_source, year
    ) a;
    
      -----------------------------------
-----month_year_id
------------------------------------
   
   
   insert into qa_reporting.pharmacy_column_checks (
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
                when month_year_id between 200701 and (extract (year from current_date)::int::text || '12')::int
                    then 1
                end),0) as validvalues,
        coalesce(sum(case
                    when month_year_id not between 200701 and (extract (year from current_date)::int::text || '12')::int
                        or month_year_id is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.pharmacy_claims 
    group by data_source, year
    ) a;
    
-----------------------------------
-----generic_ind
------------------------------------
  
      insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'generic_ind' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce (sum(case
                when generic_ind in ('0','1')
                    then 1
                end),0) as validvalues,
        coalesce(sum(case
                    when generic_ind not in ('0','1')
                        or generic_ind is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.pharmacy_claims 
    group by data_source, year
    ) a;
    
   
   -------------
   ------- generic_name
   -----------------------
   
   insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'generic_name' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select  coalesce(sum(case
                when generic_name ~ '^[[:alnum:]]{0,}$' 
                  or generic_name is null 
                    then 1
                end),0) as validvalues,
        coalesce(sum(case
                    when generic_name !~ '^[[:alnum:]]{0,}$'
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.pharmacy_claims  
    group by data_source, year
    ) a;
    
   
   
   
   -------------
   ------- brand_name
   -----------------------
   
   insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'brand_name' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select  coalesce(sum(case
                when brand_name ~ '^.{0,50}$'
                  or brand_name is null 
                    then 1
                end),0) as validvalues,
        coalesce(sum(case
                    when brand_name !~ '^.{0,50}$' and brand_name is not null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.pharmacy_claims  
    group by data_source, year
    ) a;
    
   
   
   -------------
   ------- quantity
   -----------------------
   
   insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'quantity' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select  coalesce(sum(case
                when quantity between 0 and 100 
                then 1
                end),0) as validvalues,
        coalesce(sum(case
                    when quantity not between 0 and 100 
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.pharmacy_claims  
    group by data_source, year
    ) a;
    
     -------------
   ------- provider_npi
   -----------------------
   
   insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'provider_npi' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select  coalesce(sum(case
                when provider_npi ~ '^[[:alnum:]]{10}$' 
                  or provider_npi is null 
                    then 1
                end),0) as validvalues,
        coalesce(sum(case
                    when provider_npi !~ '^[[:alnum:]]{10}$'
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.pharmacy_claims  
    group by data_source, year
    ) a;
   
   
    -------------
   ------- pharmacy_id
   -----------------------
   
   insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'pharmacy_id' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select  coalesce(sum(case
                when pharmacy_id ~ '^[[:alnum:]]{9}$' 
                  or pharmacy_id is null 
                    then 1
                end),0) as validvalues,
        coalesce(sum(case
                    when pharmacy_id !~ '^[[:alnum:]]{9}$' and pharmacy_id is not null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.pharmacy_claims  
    group by data_source, year
    ) a;
   
   
    
   
   ------------------------------------
--total_charge_amount
------------------------------------


insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'total_charge_amount' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce(sum(case
                when total_charge_amount < 1000000 or total_charge_amount is null then 1 end
                ),0)  as validvalues,
        coalesce(sum(case
                    when total_charge_amount > 1000000
                        then 1
                    end), 0)  as invalidvalues,
        year,
        data_source
    from dw_staging.pharmacy_claims
    group by data_source, year
    ) a;

----------------
--total_allowed_amount
----------------

insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'total_allowed_amount' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce(sum(case
                when total_allowed_amount < 1000000 or total_allowed_amount is null then 1 end
                ),0)  as validvalues,
        coalesce(sum(case
                    when total_allowed_amount > 1000000
                        then 1
                    end), 0)  as invalidvalues,
        year,
        data_source
    from dw_staging.pharmacy_claims
    group by data_source, year
    ) a;


------------------------------------
--total_paid_amount
------------------------------------

insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'total_paid_amount' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce(sum(case
                when total_paid_amount < 1000000 or total_paid_amount is null then 1 end
                ),0)  as validvalues,
        coalesce(sum(case
                    when total_paid_amount > 1000000
                        then 1
                    end), 0)  as invalidvalues,
        year,
        data_source
    from dw_staging.pharmacy_claims
    group by data_source, year
    ) a;
    
    ------------------------------------
--deductible
------------------------------------
--- could goto source values and see what those highest ones are 
   
insert into qa_reporting.pharmacy_column_checks (
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
    from dw_staging.pharmacy_claims
    group by data_source, year
    ) a;


------------------------------------
--coins
------------------------------------

insert into qa_reporting.pharmacy_column_checks (
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
    from dw_staging.pharmacy_claims
    group by data_source, year
    ) a;


------------------------------------
--cob
------------------------------------

insert into qa_reporting.pharmacy_column_checks (
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
    from dw_staging.pharmacy_claims
    group by data_source, year
    ) a;

-----------------------------------
-----fiscal_year
------------------------------------

insert into qa_reporting.pharmacy_column_checks (
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
				when fiscal_year between 2007 and "year" + 1 or fiscal_year is null
					then 1
				end),0) as validvalues,
		coalesce(sum(case
					when fiscal_year not between 2007 and "year" + 1
						and fiscal_year is not null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from dw_staging.pharmacy_claims 
	group by data_source,
		year
	) a;
	

-----------------------------------
-----cost_factor_year
------------------------------------

insert into qa_reporting.pharmacy_column_checks (
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
	select coalesce (sum(case
				when cost_factor_year between 2007 and extract( year from current_date )::int or cost_factor_year is null
					then 1
				end),0) as validvalues,
		coalesce(sum(case
					when cost_factor_year not between 2007  and extract( year from current_date )::int
						and cost_factor_year is not null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from dw_staging.pharmacy_claims 
	group by data_source,
		year
	) a;
	

-----------------------------------
-----therapeutic_class
------------------------------------

insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'therapeutic_class' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select coalesce (sum(case
				when therapeutic_class ~ '^\d{10}$'
					then 1
				end),0) as validvalues,
		coalesce(sum(case
					when therapeutic_class !~ '^\d{10}$'
						and therapeutic_class is not null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from dw_staging.pharmacy_claims 
	group by data_source,
		year
	) a;
	

-----------------------------------
-----ahfs_class
------------------------------------

insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'ahfs_class' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select coalesce (sum(case
				when ahfs_class ~ '^\d{6}$'
					then 1
				end),0) as validvalues,
		coalesce(sum(case
					when ahfs_class !~ '^\d{6}$'
						and ahfs_class is not null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from dw_staging.pharmacy_claims 
	group by data_source,
		year
	) a;

--------------
-----first fill
--------------
	
  insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'first_fill' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce (sum(case
                when first_fill in ('Y','N')
                    then 1
                end),0) as validvalues,
        coalesce(sum(case
                    when first_fill not in ('Y','N')
                        or first_fill is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.pharmacy_claims 
    group by data_source, year
    ) a;
    
   
   ---------------
   -----retail_or_mail_indicator
   ---------------------
   
   insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'retail_or_mail_indicator' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce (sum(case
                when retail_or_mail_indicator in ('Y','N','U')
                    then 1
                end),0) as validvalues,
        coalesce(sum(case
                    when retail_or_mail_indicator not in ('Y','N','U')
                        or retail_or_mail_indicator is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.pharmacy_claims 
    group by data_source, year
    ) a;
   
   -----------------------------------
-----dispensed_as_written
------------------------------------

insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'dispensed_as_written' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select coalesce (sum(case
				when dispensed_as_written ~ '^[[:alnum:]]{2}$'
					then 1
				end),0) as validvalues,
		coalesce(sum(case
					when dispensed_as_written !~ '^[[:alnum:]]{2}$'
						and dispensed_as_written is not null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from dw_staging.pharmacy_claims 
	group by data_source,
		year
	) a;
    
   -----------------------------------
-----dose
------------------------------------

insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'dose' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select coalesce (sum(case
				when dose ~ '^[[:alnum:]]{50}$'
					then 1
				end),0) as validvalues,
		coalesce(sum(case
					when dose !~ '^[[:alnum:]]{50}$'
						and dose is not null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from dw_staging.pharmacy_claims 
	group by data_source,
		year
	) a;
    

   -----------------------------------
-----strength
------------------------------------

insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'strength' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select coalesce (sum(case
				when strength ~ '^.*$' or strength is null
					then 1
				end),0) as validvalues,
		coalesce(sum(case
					when strength !~ '^.*$'
						and strength is not null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from dw_staging.pharmacy_claims 
	group by data_source,
		year
	) a;


---------------
   -----formulary_ind
   ---------------------
   
   insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'formulary_ind' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce (sum(case
                when formulary_ind in ('Y','N')
                    then 1
                end),0) as validvalues,
        coalesce(sum(case
                    when formulary_ind not in ('Y','N')
                        or formulary_ind is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.pharmacy_claims 
    group by data_source, year
    ) a;
    
   
   
---------------
   -----special_drug_ind
   ---------------------
   
   insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'special_drug_ind' as test_var,
    validvalues,
    invalidvalues,
    invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
    ((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select coalesce (sum(case
                when special_drug_ind in ('Y','N')
                    then 1
                end),0) as validvalues,
        coalesce(sum(case
                    when special_drug_ind not in ('Y','N')
                        or special_drug_ind is null
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.pharmacy_claims 
    group by data_source, year
    ) a;
    
   
-------------------
---- rx_claim_id_src

with ut_claim_id_table
as (
    select a.rx_claim_id_src, a."year", a.data_source, b.rx_claim_id_src as dim_id
    from dw_staging.pharmacy_claims a
    left join data_warehouse.dim_uth_rx_claim_id b on a.rx_claim_id_src = b.rx_claim_id_src 
    )
insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'rx_claim_id_src' as test_var,
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
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
-----uth_member_id_src---------------
---------------------------------
    
with ut_id_table
as (
    select a.member_id_src, a."year", a.data_source, b.member_id_src as dim_id
    from dw_staging.pharmacy_claims a
    left join data_warehouse.dim_uth_member_id b on a.member_id_src = b.member_id_src
    )
insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'member_id_src' as test_var,
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
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
--table_id_src
------------------------------------   

insert into qa_reporting.pharmacy_column_checks (
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
				'chip','ffs','mco', 'pde_file','rx', 'mdcrd','ccaed'
				)
                    then 1
                end) as validvalues,
        coalesce(sum(case
                    when table_id_src not in ('chip','ffs','mco', 'pde_file','rx', 'mdcrd','ccaed') 
                    		or table_id_src is null 
                        then 1
                    end), 0) as invalidvalues,
        year,
        data_source
    from dw_staging.pharmacy_claims 
    group by data_source , year
    ) a;
   
---------------------------------   
-----uth_script_id---------------
---------------------------------
   
  insert into qa_reporting.pharmacy_column_checks (
    test_var,
    validvalues,
    invalidvalues,
    percent_invalid,
    pass_threshold,
    "year",
    data_source,
    note
    )
select 'uth_script_id' as test_var,
	validvalues,
	invalidvalues,
	invalidvalues / (validvalues + invalidvalues)::numeric as percent_invalid,
	((invalidvalues / (validvalues + invalidvalues)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select coalesce (sum(case
				when uth_script_id is not null
					then 1
				end),0) as validvalues,
		coalesce(sum(case
					when uth_script_id is  null
						then 1
					end), 0) as invalidvalues,
		year,
		data_source
	from dw_staging.pharmacy_claims 
	group by data_source,
		year
	) a;
	
