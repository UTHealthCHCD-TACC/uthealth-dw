





/*

drop table if exists qa_reporting.data_source_check;

create table qa_reporting.data_source_check
(
schema_ text,
table_ text,
sources_ text
);

*/



delete from qa_reporting.data_source_check where schema_ = 'dw';

--- header 
with data_sources as
(
		select data_source
		from data_warehouse.claim_header
		group by data_source
)
insert into qa_reporting.data_source_check
select 'dw',
       'claim_header',
			 string_agg(data_source, ', ' order by data_source)
  from data_sources
;


---claim detail
with data_sources as
(
		select data_source
		from data_warehouse.claim_detail
		group by data_source
)
insert into qa_reporting.data_source_check
select 'dw',
       'claim_detail',
			 string_agg(data_source, ', ' order by data_source)
  from data_sources
;


--- claim diag 
with data_sources as
(
		select data_source
		from data_warehouse.claim_diag
		group by data_source
)
insert into qa_reporting.data_source_check
select 'dw',
       'claim_diag',
			 string_agg(data_source, ', ' order by data_source)
  from data_sources
;


--- procedure table 
with data_sources as
(
		select data_source
		from data_warehouse.claim_icd_proc 
		group by data_source
)
insert into qa_reporting.data_source_check
select 'dw',
       'claim_icd_proc',
			 string_agg(data_source, ', ' order by data_source)
  from data_sources
;


--- enrollment monthly 
with data_sources as
(
		select data_source
		from data_warehouse.member_enrollment_monthly 
		group by data_source
)
insert into qa_reporting.data_source_check
select 'dw',
       'member_enrollment_monthly',
			 string_agg(data_source, ', ' order by data_source)
  from data_sources
;


--- enrollment yearly 
with data_sources as
(
		select data_source
		from data_warehouse.member_enrollment_yearly
		group by data_source
)
insert into qa_reporting.data_source_check
select 'dw',
       'member_enrollment_yearly',
			 string_agg(data_source, ', ' order by data_source)
  from data_sources
;


--- pharmacy claims 
with data_sources as
(
		select data_source
		from data_warehouse.pharmacy_claims 
		group by data_source
)
insert into qa_reporting.data_source_check
select 'dw',
       'pharmacy_claims',
			 string_agg(data_source, ', ' order by data_source)
  from data_sources
;