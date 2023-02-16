delete from qa_reporting.data_source_check where schema_ = 'staging';


select * from qa_reporting.data_source_check;


--- header 
with data_sources as
(
		select data_source
		from dw_staging.claim_header
		group by data_source
)
insert into qa_reporting.data_source_check
select 'staging',
       'claim_header',
			 string_agg(data_source, ', ' order by data_source)
  from data_sources
;


---claim detail
with data_sources as
(
		select data_source
		from dw_staging.claim_detail
		group by data_source
)
insert into qa_reporting.data_source_check
select 'staging',
       'claim_detail',
			 string_agg(data_source, ', ' order by data_source)
  from data_sources
;


--- claim diag 
with data_sources as
(
		select data_source
		from dw_staging.claim_diag
		group by data_source
)
insert into qa_reporting.data_source_check
select 'staging',
       'claim_diag',
			 string_agg(data_source, ', ' order by data_source)
  from data_sources
;


--- procedure table 
with data_sources as
(
		select data_source
		from dw_staging.claim_icd_proc 
		group by data_source
)
insert into qa_reporting.data_source_check
select 'staging',
       'claim_icd_proc',
			 string_agg(data_source, ', ' order by data_source)
  from data_sources
;


--- enrollment monthly 
with data_sources as
(
		select data_source
		from dw_staging.member_enrollment_monthly 
		group by data_source
)
insert into qa_reporting.data_source_check
select 'staging',
       'member_enrollment_monthly',
			 string_agg(data_source, ', ' order by data_source)
  from data_sources
;


--- enrollment yearly 
with data_sources as
(
		select data_source
		from dw_staging.member_enrollment_yearly
		group by data_source
)
insert into qa_reporting.data_source_check
select 'staging',
       'member_enrollment_yearly',
			 string_agg(data_source, ', ' order by data_source)
  from data_sources
;


--- pharmacy claims 
with data_sources as
(
		select data_source
		from dw_staging.pharmacy_claims 
		group by data_source
)
insert into qa_reporting.data_source_check
select 'staging',
       'pharmacy_claims',
			 string_agg(data_source, ', ' order by data_source)
  from data_sources
;