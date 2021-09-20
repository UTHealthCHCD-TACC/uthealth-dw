/*
 *
 *
--------------------------------------------------------------------------------

--********************************************----------------------------------
--------Claim header QA quarantine table
--********************************************----------------------------------

--------------------------------------------------------------------------------



--- jw001 | 9/10/2021 | script creation

*/


drop table if exists qa_reporting.claim_header_quarantine;

create table qa_reporting.claim_header_quarantine 
with(appendonly = true, orientation = column, compresstype = zlib)
as
select *
from data_warehouse.claim_header
limit 0 
distributed by (uth_member_id);

vacuum analyze  qa_reporting.claim_header_quarantine;

------------------------------------
--------data source---------
------------------------------------

alter table qa_reporting.claim_header_quarantine 
  add column data_source_flag INT; 

insert into qa_reporting.claim_header_quarantine 
(
data_source,
"year",
uth_claim_id,
uth_member_id,
from_date_of_service,
claim_type,
uth_admission_id,
admission_id_src,
total_charge_amount,
total_allowed_amount,
total_paid_amount,
claim_id_src,
member_id_src,
table_id_src,
fiscal_year,
cost_factor_year,
to_date_of_service,
data_source_flag
) 
select 
		data_source,
		"year",
		uth_claim_id,
		uth_member_id,
		from_date_of_service,
		claim_type,
		uth_admission_id,
		admission_id_src,
		total_charge_amount,
		total_allowed_amount,
		total_paid_amount,
		claim_id_src,
		member_id_src,
		table_id_src,
		fiscal_year,
		cost_factor_year,
		to_date_of_service,
       1 as data_source_flag 
from   data_warehouse.claim_header
where  data_source not in ( 'mcrt', 'optz', 'mdcd', 'mcrn', 
                            'truv', 'optd' );   

------------------------------------
--------year--------------
------------------------------------
                           
alter table qa_reporting.claim_header_quarantine 
  add column year_flag INT; 

insert into qa_reporting.claim_header_quarantine 
(
data_source,
"year",
uth_claim_id,
uth_member_id,
from_date_of_service,
claim_type,
uth_admission_id,
admission_id_src,
total_charge_amount,
total_allowed_amount,
total_paid_amount,
claim_id_src,
member_id_src,
table_id_src,
fiscal_year,
cost_factor_year,
to_date_of_service,
year_flag
)
select 
				data_source,
				"year",
				uth_claim_id,
				uth_member_id,
				from_date_of_service,
				claim_type,
				uth_admission_id,
				admission_id_src,
				total_charge_amount,
				total_allowed_amount,
				total_paid_amount,
				claim_id_src,
				member_id_src,
				table_id_src,
				fiscal_year,
				cost_factor_year,
				to_date_of_service,
       1 as year_flag 
from   data_warehouse.claim_header
where  year not between 2007 and 2020 
        or year is null;   


    
    
---------------------------------   
-----uth_claim_id---------------
---------------------------------
    
-------------------------check 1: in dim table  
alter table qa_reporting.claim_header_quarantine 
add column uth_claim_id_flag int;

with ut_claim_id_table
as (
    select a.*, b.uth_claim_id as dim_id
    from data_warehouse.claim_header a
    left join data_warehouse.dim_uth_claim_id b on a.uth_claim_id = b.uth_claim_id
    )
insert into qa_reporting.claim_header_quarantine 
(
				data_source,
				"year",
				uth_claim_id,
				uth_member_id,
				from_date_of_service,
				claim_type,
				uth_admission_id,
				admission_id_src,
				total_charge_amount,
				total_allowed_amount,
				total_paid_amount,
				claim_id_src,
				member_id_src,
				table_id_src,
				fiscal_year,
				cost_factor_year,
				to_date_of_service,
				uth_claim_id_flag
)
select 
				data_source,
				"year",
				uth_claim_id,
				uth_member_id,
				from_date_of_service,
				claim_type,
				uth_admission_id,
				admission_id_src,
				total_charge_amount,
				total_allowed_amount,
				total_paid_amount,
				claim_id_src,
				member_id_src,
				table_id_src,
				fiscal_year,
				cost_factor_year,
				to_date_of_service,
       1 as uth_claim_id_flag 
from  ut_claim_id_table 
where dim_id is null;  


---------------------------------	
-----uth_member_id---------------
---------------------------------
	
alter table qa_reporting.claim_header_quarantine 
add column member_id_flag int;


with ut_id_table
as (
    select a.*, b.uth_member_id as src_id
    from data_warehouse.claim_header a
    left join data_warehouse.dim_uth_member_id b on a.uth_member_id = b.uth_member_id
    )
insert into qa_reporting.claim_header_quarantine 
            ( 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
						member_id_flag
            ) 
select 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
						1 as member_id_flag 
from   ut_id_table 
where  src_id is null ;  


------------------------------------
--------from_date_of_service--------
------------------------------------

alter table qa_reporting.claim_header_quarantine 
  add column from_date_of_service_flag INT; 

insert into qa_reporting.claim_header_quarantine 
            ( 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
						from_date_of_service_flag
            ) 
select 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
       1 as from_date_of_service_flag 
from   data_warehouse.claim_header
where  from_date_of_service not between '2007-01-01' and current_date
                        or from_date_of_service is null ;
    
               
                       
------------------------------------
--------to_date_of_service--------
------------------------------------

                       
alter table qa_reporting.claim_header_quarantine 
  add column to_date_of_service_flag INT; 

insert into qa_reporting.claim_header_quarantine 
            ( 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
						to_date_of_service_flag
            ) 
						select 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
						1 as to_date_of_service_flag 
from   data_warehouse.claim_header
where  to_date_of_service not between '2007-01-01' and current_date
                        and to_date_of_service is not null ;
                       
                       
                       
------------------------------------
--------claim_type--------
------------------------------------

                       
alter table qa_reporting.claim_header_quarantine 
  add column claim_type_flag INT; 

insert into qa_reporting.claim_header_quarantine 
            ( 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
						claim_type_flag
            ) 
						select 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
						1 as claim_type_flag 
from   data_warehouse.claim_header
where  claim_type !~ '^(F|P)$' or claim_type is null;
                       
                       
                               

                       
                       
                       
------------------------------------
--------uth_admission_id--------
------------------------------------

                       
alter table qa_reporting.claim_header_quarantine 
  add column admission_flag INT; 

with ut_admission_id_table
as (
    select a.*, b.uth_admission_id as dim_admit
    from data_warehouse.claim_header a
    left join data_warehouse.dim_uth_admission_id b on a.uth_admission_id = b.uth_admission_id 
    where a.uth_admission_id is not null
    )
insert into qa_reporting.claim_header_quarantine 
            ( 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
						admission_flag
            ) 
						select 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
						1 as admission_flag 
from   ut_admission_id_table
where  dim_admit is null;                       
                       
                       
                       
                       
                       
                       
---------------------------------	
-----admission_id_src--------
---------------------------------                       
                       
                       
alter table qa_reporting.claim_header_quarantine 
  add column admission_id_src_flag INT; 

with ut_admission_id_table_src
as (
    select a.*,
    				b.admission_id_src as dim_admit
     	from data_warehouse.claim_header a
  	  left join data_warehouse.dim_uth_admission_id b 
   				 on a.admission_id_src::text = b.admission_id_src
     where a.admission_id_src is not null
    )
insert into qa_reporting.claim_header_quarantine 
            ( 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
						admission_id_src_flag
            ) 
						select 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
						1 as admission_id_src_flag 
from   ut_admission_id_table_src
where  dim_admit is null;                         
                       
                       
------------------------------------
--------total_charge_amount--------
------------------------------------

                       
alter table qa_reporting.claim_header_quarantine 
  add column total_charge_amount_flag INT; 

insert into qa_reporting.claim_header_quarantine 
            ( 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
						total_charge_amount_flag
            ) 
						select 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
						1 as total_charge_amount_flag 
from   data_warehouse.claim_header
where  total_charge_amount > 1000000;                       
                       
                       
                       
                       
------------------------------------
--------total_allowed_amount--------
------------------------------------

                       
alter table qa_reporting.claim_header_quarantine 
  add column total_allowed_amount_flag INT; 

insert into qa_reporting.claim_header_quarantine 
            ( 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
						total_allowed_amount_flag
            ) 
						select 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
						1 as total_allowed_amount_flag 
		from   data_warehouse.claim_header
		where  total_allowed_amount > 1000000;                                 
                       
                       
                       
                       
------------------------------------
--------total_paid_amount--------
------------------------------------

                       
alter table qa_reporting.claim_header_quarantine 
  add column total_paid_amount_flag INT; 

insert into qa_reporting.claim_header_quarantine 
            ( 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
						total_allowed_amount_flag
            ) 
						select 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
						1 as total_allowed_amount_flag 
from   data_warehouse.claim_header
where  total_paid_amount > 1000000;                          
                       
                       
                       
                       
------------------------------------
--------claim id source-------------
------------------------------------                       
                       
                       
alter table qa_reporting.claim_header_quarantine 
  add column claim_id_source_flag INT;                        
                       
 
with src_table
as (
	select a.*, b.id_src as id_src
	from data_warehouse.claim_header a
	left join dev.qa_claim_header_temp_idsrc b on a.claim_id_src = b.id_src
	)
insert into qa_reporting.claim_header_quarantine 
            ( 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
						claim_id_source_flag
            ) 
						select 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
						1 as claim_id_source_flag 	
				from src_table 
			 where id_src is null;
	
	
	
	
	
	
	
	
------------------------------------
--------member id source-------------
------------------------------------   
                    
alter table qa_reporting.claim_header_quarantine 
  add column member_id_flag INT;                        
                       
 
with ut_id_table
as (
    select a.*, c.id_src as id_src 
    from data_warehouse.claim_header a
    left outer join dev.qa_claim_header_temp_member_idsrc c 
        on a.member_id_src = c.id_src
    ) 
insert into qa_reporting.claim_header_quarantine 
            ( 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
						member_id_flag
            ) 
						select 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
						1 as member_id_flag 	
				from ut_id_table 
			 where id_src is null;                       
                       
			
------------------------------------
--------table_id_src--------
------------------------------------

                       
alter table qa_reporting.claim_header_quarantine 
  add column table_id_src_flag INT; 

insert into qa_reporting.claim_header_quarantine 
            ( 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
						table_id_src_flag
            ) 
						select 
						data_source,
						"year",
						uth_claim_id,
						uth_member_id,
						from_date_of_service,
						claim_type,
						uth_admission_id,
						admission_id_src,
						total_charge_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_id_src,
						member_id_src,
						table_id_src,
						fiscal_year,
						cost_factor_year,
						to_date_of_service,
						1 as table_id_src_flag 
from   data_warehouse.claim_header
where  table_id_src not in ('outpatient_base_claims_k','medical',
'hha_base_claims_k','enc_header','ccaeo','dme_claims_k','snf_base_claims_k',
'mdcrs','mdcro','inpatient_base_claims_k','ccaes','bcarrier_claims_k',
'clm_header','hospice_base_claims_k')               ;                
                       
                       
                       
-----------------------------------
-----fiscal_year
------------------------------------                       

                           
alter table qa_reporting.claim_header_quarantine 
  add column fiscal_year_flag INT; 

insert into qa_reporting.claim_header_quarantine 
(
				data_source,
				"year",
				uth_claim_id,
				uth_member_id,
				from_date_of_service,
				claim_type,
				uth_admission_id,
				admission_id_src,
				total_charge_amount,
				total_allowed_amount,
				total_paid_amount,
				claim_id_src,
				member_id_src,
				table_id_src,
				fiscal_year,
				cost_factor_year,
				to_date_of_service,
				fiscal_year_flag
)
select 
				data_source,
				"year",
				uth_claim_id,
				uth_member_id,
				from_date_of_service,
				claim_type,
				uth_admission_id,
				admission_id_src,
				total_charge_amount,
				total_allowed_amount,
				total_paid_amount,
				claim_id_src,
				member_id_src,
				table_id_src,
				fiscal_year,
				cost_factor_year,
				to_date_of_service,
       1 as fiscal_year_flag 
from   data_warehouse.claim_header
where  fiscal_year not between 2007 and 2020 
        or year is null;                          
 
       
       
       
-----------------------------------
-----cost_factor_year
------------------------------------                       

                           
alter table qa_reporting.claim_header_quarantine 
  add column cost_factor_year_flag INT; 

insert into qa_reporting.claim_header_quarantine 
(
				data_source,
				"year",
				uth_claim_id,
				uth_member_id,
				from_date_of_service,
				claim_type,
				uth_admission_id,
				admission_id_src,
				total_charge_amount,
				total_allowed_amount,
				total_paid_amount,
				claim_id_src,
				member_id_src,
				table_id_src,
				fiscal_year_flag,
				cost_factor_year,
				to_date_of_service,
				cost_factor_year_flag
)
select 
				data_source,
				"year",
				uth_claim_id,
				uth_member_id,
				from_date_of_service,
				claim_type,
				uth_admission_id,
				admission_id_src,
				total_charge_amount,
				total_allowed_amount,
				total_paid_amount,
				claim_id_src,
				member_id_src,
				table_id_src,
				fiscal_year,
				cost_factor_year,
				to_date_of_service,
       1 as cost_factor_year_flag 
from   data_warehouse.claim_header
where  (cost_factor_year not between 2007 and 2020
						and cost_factor_year is not null);   
       
       
       
       
       
       
       
       
       
       
       
       
       
       
       


select * from qa_reporting.claim_header_quarantine
where data_source_flag = 1;

select * from qa_reporting.claim_header_quarantine
where year_flag = 1;

-- fiscal year is normal but year is higher
-- lopita said some claims are older 
-- should we say ... if not in range and fiscal year in range then fine? 


select * from qa_reporting.claim_header_quarantine
where uth_claim_id_flag = 1;


select * from qa_reporting.claim_header_quarantine
where member_id_flag = 1;


select * from qa_reporting.claim_header_quarantine
where from_date_of_service_flag = 1;

select * from qa_reporting.claim_header_quarantine
where to_date_of_service_flag = 1
and data_source = 'truv';


select * from qa_reporting.claim_header_quarantine
where claim_type_flag = 1;

select * from qa_reporting.claim_header_quarantine
where admission_flag = 1;

select * from qa_reporting.claim_header_quarantine
where admission_id_src_flag = 1;


select * from qa_reporting.claim_header_quarantine
where total_charge_amount_flag = 1;

select * from qa_reporting.claim_header_quarantine
where total_allowed_amount = 1;


select * from qa_reporting.claim_header_quarantine
where claim_id_source_flag = 1;


select * from qa_reporting.claim_header_quarantine
where member_id_flag = 1;

select * from qa_reporting.claim_header_quarantine
where table_id_src_flag = 1;


select * from qa_reporting.claim_header_quarantine
where fiscal_year_flag = 1;

select * from qa_reporting.claim_header_quarantine
where cost_factor_year = 1;

*/


   