-- overall enrollment - just gets general enrollment information (no conditions, crg risk score, covid severity, etc.)

create or replace view tableau.enrollment_view
as
select data_source, year, uth_member_id, gender_cd, age_derived, state, plan_type, race_cd, bus_cd, total_enrolled_months
from tableau.master_enrollment a ;


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

-- Covid Severity

create or replace view tableau.cov_severity_view
as
select data_source, uth_member_id, year, cov_severity, gender_cd, age_derived, plan_type, bus_cd, state
  from tableau.master_enrollment
 where year >= 2020;
 
-- DW Dashboard

CREATE OR REPLACE VIEW tableau.dw_dashboard
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
     LEFT JOIN tableau.master_claims b ON a.uth_member_id = b.uth_member_id AND a.data_source::text = b.data_source AND a.year = b.year;

-- Conditions

CREATE OR REPLACE VIEW tableau.member_conditions_pivoted_view
AS SELECT a.data_source,
    a.year,
    a.uth_member_id,
    t.condition_name,
    t.condition_flag
   FROM tableau.master_enrollment a
     CROSS JOIN LATERAL ( VALUES ('aimm'::text,a.aimm), ('ami'::text,a.ami), ('ca'::text,a.ca), ('cfib'::text,a.cfib), ('chf'::text,a.chf), ('ckd'::text,a.ckd), ('cliv'::text,a.cliv), ('copd'::text,a.copd), ('cysf'::text,a.cysf), ('dep'::text,a.dep), ('epi'::text,a.epi), ('fbm'::text,a.fbm), ('hemo'::text,a.hemo), ('hep'::text,a.hep), ('hiv'::text,a.hiv), ('ihd'::text,a.ihd), ('lbp'::text,a.lbp), ('lymp'::text,a.lymp), ('ms'::text,a.ms), ('nicu'::text,a.nicu), ('pain'::text,a.pain), ('park'::text,a.park), ('pneu'::text,a.pneu), ('ra'::text,a.ra), ('scd'::text,a.scd), ('smi'::text,a.smi), ('str'::text,a.str), ('tbi'::text,a.tbi), ('trans'::text,a.trans), ('trau'::text,a.trau), ('N/A'::text,a.aimm)) t(condition_name, condition_flag)
  WHERE (t.condition_flag = 1::smallint AND t.condition_name <> 'N/A'::text) OR t.condition_name = 'N/A'::text;