select min(age_derived), max(age_derived)
from data_warehouse.member_enrollment_monthly;

select distinct gender_cd
from data_warehouse.member_enrollment_monthly;

select distinct race_cd
from data_warehouse.member_enrollment_monthly;

ALTER TABLE data_warehouse.member_enrollment_monthly 
DROP CONSTRAINT valid_value_constraints;

ALTER TABLE data_warehouse.member_enrollment_monthly 
ADD CONSTRAINT valid_value_constraints 
CHECK (
	age_derived >= 0
	AND age_derived < 125
	AND (gender_cd='F' or gender_cd='M' or gender_cd='U')
	and (bus_cd='COM' or bus_cd='MCR' or bus_cd='MDCR' or bus_cd='MCD')
	and ((race_cd between 0 and 6) or race_cd is null)) 
);