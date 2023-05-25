-- order in which to drop views before creating them again
drop view tableau.crg_risk_view;
drop view tableau.covid_severity_view;
drop view tableau.member_condition_view;
drop view tableau.member_conditions_pivoted_view;
drop view tableau.dashboard_1720_view;
drop view tableau.enrollment_view;

-- creating view, must follow order in which the queries show up (some of the views depend on oher views creatd before)

-- overall enrollment - just gets general enrollment information (no conditions, crg risk score, covid severity, etc.)
create or replace view tableau.enrollment_view
as
select data_source, year, uth_member_id, gender_cd, age_derived, state, plan_type, race_cd, bus_cd, total_enrolled_months
from tableau.master_enrollment a;

-- DW Dashboard

CREATE OR REPLACE VIEW tableau.dashboard_1720_view
AS SELECT a.data_source,
    a.year,
    a.uth_member_id,
    a.total_enrolled_months,
    a.gender_cd,
    a.age_derived,
    a.state,
    a.plan_type,
    a.bus_cd,
    b.uth_claim_id,
    b.claim_type,
    b.total_charge_amount,
    b.total_allowed_amount,
    b.total_paid_amount
FROM tableau.enrollment_view a
LEFT JOIN tableau.master_claims b 
ON a.uth_member_id = b.uth_member_id 
AND a.data_source::text = b.data_source
AND a.year = b.year;

-- Conditions
CREATE OR REPLACE VIEW tableau.member_conditions_pivoted_view
AS SELECT a.data_source,
    a.year,
    a.uth_member_id,
    a.gender_cd,
    a.race_cd,
    a.age_derived,
    a.bus_cd,
    a.plan_type,
    a.state,
    t.condition_name,
    t.condition_flag
FROM tableau.master_enrollment a
CROSS JOIN LATERAL ( 
VALUES ('aimm', a.aimm), ('ami', a.ami), ('ca', a.ca), ('cfib', a.cfib), ('chf', a.chf), ('ckd', a.ckd), ('cliv', a.cliv), ('copd', a.copd), 
		('cysf', a.cysf), ('dep', a.dep), ('epi', a.epi), ('fbm', a.fbm), ('hemo', a.hemo), ('hep', a.hep), ('hiv', a.hiv), 
		('ihd', a.ihd), ('lbp', a.lbp), ('lymp', a.lymp), ('ms', a.ms), ('nicu', a.nicu), ('pain', a.pain), ('park', a.park), 
		('pneu', a.pneu), ('ra', a.ra), ('scd', a.scd), ('smi', a.smi), ('str', a.str), ('tbi', a.tbi), ('trans', a.trans), 
		('trau', a.trau), ('asth', a.asth), ('dem', a.dem), ('diab', a.diab), ('htn', a.htn), ('opi', a.opi), ('tob', a.tob), ('N/A'::text, a.aimm)) 
t(condition_name, condition_flag)
WHERE (t.condition_flag = 1::smallint AND t.condition_name <> 'N/A'::text) 
OR t.condition_name = 'N/A'::text;

CREATE OR REPLACE VIEW tableau.member_condition_view
AS 
SELECT data_source, year, uth_member_id, gender_cd, race_cd,
    age_derived, state, plan_type, bus_cd, total_enrolled_months,
    a.aimm, a.ami, a.ca, a.cfib, a.chf, a.ckd, a.cliv, a.copd, 
    a.cysf, a.dep, a.epi, a.fbm, a.hemo, a.hep, a.hiv, 
    a.ihd, a.lbp, a.lymp, a.ms, a.nicu, a.pain, a.park, 
    a.pneu, a.ra, a.scd, a.smi, a.str, a.tbi, a.trans, 
    a.trau, a.asth, a.dem, a.diab, a.htn, a.opi, a.tob, 
FROM tableau.master_enrollment a
; 

-- Covid Severity
create or replace view tableau.covid_severity_view
as
select data_source, uth_member_id, year, covid_severity, gender_cd, age_derived, plan_type, bus_cd, state
  from tableau.master_enrollment
 where year >= 2020
and covid_severity is not null;    
    
-- CRG
create or replace view tableau.crg_risk_view
as
select
	b.data_source,
    b.year as crg_year,
    b.uth_member_id,
    b.gender_cd,
    b.age_derived,
    b.plan_type,
    b.bus_cd,
    b.state,
    b.race_cd,
    b.total_enrolled_months,
    b.crg,
    b.crg_abbreviated,
	a.condition_name,
    a.condition_flag,
    CASE
        WHEN a.condition_name = 'N/A'::text THEN 'Without Condition'::text
        ELSE 'With Condition'::text
    END AS with_without_condition
from tableau.member_conditions_pivoted_view a
left join tableau.master_enrollment b
  on a.uth_member_id = b.uth_member_id
 and a.data_source = b.data_source
 and a.year = b.year;  


 
-- granting select permissions to uthealth_analyst for all view 
grant select on tableau.member_conditions_pivoted_view to uthealth_analyst; 
grant select on tableau.crg_risk_view to uthealth_analyst; 
grant select on tableau.member_condition_view to uthealth_analyst; 
grant select on tableau.dashboard_1720_view to uthealth_analyst; 
grant select on tableau.enrollment_view to uthealth_analyst; 
grant select on tableau.covid_severity_view to uthealth_analyst; 
