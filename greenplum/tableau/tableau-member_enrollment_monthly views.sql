

create materialized view tableau.enrollment_optd_test as (


	select b.start_of_month,
		   b.end_of_month,
		   uth_member_id
	from data_warehouse.member_enrollment_monthly a
	  join reference_tables.ref_month_year b
	    on b.month_year_id = a.month_year_id
	 --  and b.year_int = 2015
	where data_source = 'optd'
	  and uth_member_id = 20160316714
	limit 100
	;
	
); 



select * 
from medicare.mbsf_abcd_summary
limit 10
;

