/*
 *
 *
--------------------------------------------------------------------------------
--********************************************----------------------------------
--------   data_warehouse.claim_header Column QA ------------------
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
	from data_warehouse.claim_header
	group by data_source,
		year
	) a;


------------------------------------
--------year--------------
------------------------------------
-- add which years it should be? 

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
	from data_warehouse.claim_header
	group by data_source,
		year
	) a;
	


---------------------------------
-----uth_claim_id----------------
---------------------------------


with ut_claim_id_table
as (
    select a.uth_claim_id, a."year", a.data_source, b.uth_claim_id as dim_id
    from data_warehouse.claim_header a
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
    from data_warehouse.claim_header a
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
    from data_warehouse.claim_header
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
    from data_warehouse.claim_header
    group by data_source, year
    ) a;   
   
   
   
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
    from data_warehouse.claim_header 
    group by data_source, year
    ) a;
   
   
---------------------------------	
-----uth_admission_id------------
---------------------------------
   
--if uth_admission does exist check	
   
with ut_admission_id_table
as (
    select b.uth_admission_id, a."year", a.data_source, b.uth_admission_id as dim_admit
    from data_warehouse.claim_header a
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
   
   
---------------------------------	
-----uth_admission_id_src------------
---------------------------------
delete from qa_reporting.claim_header_column_checks where test_var = 'admission_id_src';
select * from qa_reporting.claim_header_column_checks where test_var = 'admission_id_src';

with ut_admission_id_table_src
as (
    select a.admission_id_src , 
    				a."year", 
    				a.data_source, 
    				b.admission_id_src as dim_admit
     	from data_warehouse.claim_header a
  	  left join data_warehouse.dim_uth_admission_id b 
   				 on a.admission_id_src::text = b.admission_id_src
     where a.admission_id_src is not null
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
select 'admission_id_src' as test_var,
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
    from ut_admission_id_table_src
       group by data_source, year
    ) a;      
   
   select count(*) from data_warehouse.claim_header where admission_id_src is not null; --11049106360
   --232391865
   
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
    from data_warehouse.claim_header
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
    from data_warehouse.claim_header
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
                    when total_paid_amount > 1000000
                        then 1
                    end), 0)  as invalid_values,
        year,
        data_source
    from data_warehouse.claim_header
    group by data_source, year
    ) a;      


------------------------------------
--------claim id source-------------
------------------------------------
   
   -----
   --get all source claim ids--
   ----
   /*
drop table if exists dev.qa_claim_header_temp_idsrc;
   
select distinct id_src
into dev.qa_claim_header_temp_idsrc
from (
	select clm_id as id_src
	from medicare_national.dme_claims_k

	union all

	select clm_id as id_src
	from medicare_national.inpatient_revenue_center_k

	union all

	select clm_id as id_src
	from medicare_national.hospice_base_claims_k

	union all

	select clm_id as id_src
	from medicare_national.outpatient_revenue_center_k

	union all

	select clm_id as id_src
	from medicare_national.hha_revenue_center_k

	union all

	select clm_id as id_src
	from medicare_national.snf_base_claims_k

	union all

	select clm_id as id_src
	from medicare_national.bcarrier_claims_k

	union all

	select clm_id as id_src
	from medicare_texas.dme_claims_k

	union all

	select clm_id as id_src
	from medicare_texas.inpatient_revenue_center_k

	union all

	select clm_id as id_src
	from medicare_texas.hospice_base_claims_k

	union all

	select clm_id as id_src
	from medicare_texas.outpatient_revenue_center_k

	union all

	select clm_id as id_src
	from medicare_texas.hha_revenue_center_k

	union all

	select clm_id as id_src
	from medicare_texas.snf_base_claims_k

	union all

	select clm_id as id_src
	from medicare_texas.bcarrier_claims_k

	union all

	select derv_enc as id_src
	from medicaid.enc_det

	union all

	select icn as id_src
	from medicaid.clm_detail

	union all

	select clmid as id_src
	from optum_dod.medical

	union all

	select clmid as id_src
	from optum_zip.medical

	union all

	select msclmid::text as id_src
	from truven.ccaeo

	union all

	select msclmid::text as id_src
	from truven.mdcro

	union all

	select msclmid::text as id_src
	from truven.mdcrs

	union all

	select msclmid::text as id_src
	from truven.ccaes
	) a;
vacuum analyze dev.qa_claim_header_temp_idsrc;

*/--

with src_table
as (
	select a.*, b.id_src as id_src
	from data_warehouse.claim_header a
	left join dev.qa_claim_header_temp_idsrc b on a.claim_id_src = b.id_src
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
select 'claim_id_src' as test_var,
    valid_values,
    invalid_values,
    invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
    ((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
    year,
    data_source,
    '' as note
from (
    select sum(case
                when id_src is not null
                    then 1
                end) as valid_values,
        coalesce(sum(case
                    when id_src is null
                        then 1
                    end), 0) as invalid_values,
        year,
        data_source
    from src_table
    group by data_source, year
    ) a;


   
   
   
   
   
------------------------------------
--------member id source-------------
------------------------------------

---get all src from original tables
/*   
select distinct id_src
into dev.qa_claim_header_temp_member_idsrc
from (
	select bene_id::text as id_src
	from medicare_national.dme_claims_k

	union all

	select bene_id::text as id_src
	from medicare_national.inpatient_revenue_center_k

	union all

	select bene_id::text as id_src
	from medicare_national.hospice_base_claims_k

	union all

	select bene_id::text as id_src
	from medicare_national.outpatient_revenue_center_k

	union all

	select bene_id::text as id_src
	from medicare_national.hha_revenue_center_k

	union all

	select bene_id::text as id_src
	from medicare_national.snf_base_claims_k

	union all

	select bene_id::text as id_src
	from medicare_national.bcarrier_claims_k

	union all

	select bene_id::text as id_src
	from medicare_texas.dme_claims_k

	union all

	select bene_id::text as id_src
	from medicare_texas.inpatient_revenue_center_k

	union all

	select bene_id::text as id_src
	from medicare_texas.hospice_base_claims_k

	union all

	select bene_id::text as id_src
	from medicare_texas.outpatient_revenue_center_k

	union all

	select bene_id::text as id_src
	from medicare_texas.hha_revenue_center_k

	union all

	select bene_id::text as id_src
	from medicare_texas.snf_base_claims_k

	union all

	select bene_id::text as id_src
	from medicare_texas.bcarrier_claims_k

	union all

	select mem_id::text as id_src
	from medicaid.enc_proc

	union all

	select pcn::text as id_src
	from medicaid.clm_proc

	union all

	select patid::text as id_src
	from optum_dod.medical

	union all

	select patid::text as id_src
	from optum_zip.medical

	union all

	select enrolid::text as id_src
	from truven.ccaeo

	union all

	select enrolid::text as id_src
	from truven.mdcro

	union all

	select enrolid::text as id_src
	from truven.mdcrs

	union all

	select enrolid::text as id_src
	from truven.ccaes
	) a;
vacuum analyze dev.qa_claim_header_temp_member_idsrc;*/

   
   
with ut_id_table
as (
    select a.uth_member_id, a."year", a.data_source, a.member_id_src, c.id_src 
    from data_warehouse.claim_header a
    left outer join dev.qa_claim_header_temp_member_idsrc c 
        on a.member_id_src = c.id_src
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
                when id_src is not null
                    then 1
                end) as valid_values,
        coalesce(sum(case
                    when id_src is null
                        then 1
                    end), 0) as invalid_values,
        year,
        data_source
    from ut_id_table
    group by data_source, year
    ) a;

   
   
------------------------------------
--------table_id_src----------------
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
select 'table_id_src' as test_var,
	valid_values,
	invalid_values,
	invalid_values / (valid_values + invalid_values)::numeric as percent_invalid,
	((invalid_values / (valid_values + invalid_values)::numeric) < 0.01) as pass_threshold,
	year,
	data_source,
	'' as note
from (
	select sum(case
				when table_id_src in ('outpatient_base_claims_k','medical','hha_base_claims_k','enc_header','ccaeo','dme_claims_k','snf_base_claims_k','mdcrs','mdcro','inpatient_base_claims_k','ccaes','bcarrier_claims_k','clm_header','hospice_base_claims_k')
					then 1
				end) as valid_values,
		coalesce(sum(case
					when table_id_src not in ('outpatient_base_claims_k','medical','hha_base_claims_k','enc_header','ccaeo','dme_claims_k','snf_base_claims_k','mdcrs','mdcro','inpatient_base_claims_k','ccaes','bcarrier_claims_k','clm_header','hospice_base_claims_k')
						then 1
					end), 0) as invalid_values,
		year,
		data_source
	from data_warehouse.claim_header
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
	from data_warehouse.claim_header 
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
				when cost_factor_year between 2007 and 2020
					then 1
				end) as valid_values,
		coalesce(sum(case
					when cost_factor_year not between 2007 and 2020
						or cost_factor_year is null
						then 1
					end), 0) as invalid_values,
		year,
		data_source
	from data_warehouse.claim_header
    group by data_source, year
	) a;
	
