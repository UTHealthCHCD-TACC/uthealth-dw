-- move previous counts to other table for reference

drop table if exists qa_reporting.tableau_master_claims_count_old;

select *
into qa_reporting.tableau_master_claims_count_old
from qa_reporting.tableau_master_claims_count;

-- get counts from claim_header table and compare them with counts from master_claims table

drop table if exists qa_reporting.tableau_master_claims_count;

with dw as (
	select data_source, year, count(*) dw_row_count, count(distinct uth_claim_id) dw_claim_count, count(distinct uth_member_id) dw_member_count
	from data_warehouse.claim_header
	where year >= 2014
	group by 1,2
),
tableau as (
select data_source, year, count(*) tableau_row_count, count(distinct uth_claim_id) tableau_claim_count, count(distinct uth_member_id) tableau_member_count
from tableau.master_claims
group by 1,2
)
select a.*, dw_row_count, dw_claim_count, dw_member_count
into qa_reporting.tableau_master_claims_count
from tableau a
join dw b
on a.data_source = b.data_source
and a.year = b.year;

select *, tableau_row_count - dw_row_count, 100. * (dw_row_count - tableau_row_count) / dw_row_count
from qa_reporting.tableau_master_claims_count
where tableau_row_count != dw_row_count;

