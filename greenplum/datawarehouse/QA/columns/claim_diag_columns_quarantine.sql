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


drop table if exists qa_reporting.claim_diag_quarantine;

create table qa_reporting.claim_diag_quarantine 
with(appendonly = true, orientation = column, compresstype = zlib)
as
select *
from data_warehouse.claim_diag
limit 0 
distributed by (uth_member_id);

vacuum analyze  qa_reporting.claim_diag_quarantine;











--------------diags 



with all_diag as
(
	select code from reference_tables.icd_10_diags
	union all
	select code from reference_tables.icd_9_diags 
),
diag_table
as (
    select a."year", a.data_source, b.code as ref_diag
    from data_warehouse.claim_diag a
    left join all_diag b on a.diag_cd = b.code 
    )
insert into qa_reporting.claim_header_quarantine 
(
		data_source,
		"year",
		uth_member_id,
		uth_claim_id,
		claim_sequence_number,
		from_date_of_service,
		diag_cd,
		diag_position,
		icd_type,
		poa_src,
		fiscal_year
) 
select 
		data_source,
		"year",
		uth_member_id,
		uth_claim_id,
		claim_sequence_number,
		from_date_of_service,
		diag_cd,
		diag_position,
		icd_type,
		poa_src,
		fiscal_year,
		1 as icd_flag
from   data_warehouse.claim_header
where  data_source not in ( 'mcrt', 'optz', 'mdcd', 'mcrn', 
                            'truv', 'optd' );  