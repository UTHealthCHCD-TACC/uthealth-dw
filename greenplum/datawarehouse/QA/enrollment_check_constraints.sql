select distinct race_cd
from data_warehouse.member_enrollment_monthly;


ALTER TABLE data_warehouse.member_enrollment_monthly 
ADD CONSTRAINT valid_value_constraints 
CHECK (
	age_derived >= 0
	AND age_derived < 125
	AND (gender_cd='F' or gender_cd='M' or gender_cd='U')
	and (bus_cd='COM' or bus_cd='MCR' or bus_cd='MDCR' or bus_cd='MCD') 
);