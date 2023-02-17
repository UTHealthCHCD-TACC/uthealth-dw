create or replace view tableau.enrollment_view
as
select data_source, year, uth_member_id, gender_cd, age_derived, state, plan_type, race_cd, bus_cd, total_enrolled_months
from tableau.master_enrollment a ;
  
drop view tableau.crg_view;

create or replace view tableau.crg_view
as
select
	data_source,
	uth_member_id,
	year as crg_view,
	gender_cd,
	age_derived,
	plan_type,
	bus_cd,
	state,
	race_cd,
	total_enrolled_months,
	crg,
	crg_abbreviated
from tableau.master_enrollment;  

create or replace view tableau.cov_severity_view
as
select data_source, uth_member_id, year, cov_severity
  from tableau.master_enrollment;
 
select * from tableau.enrollment_view;

select * from tableau.crg_view;
