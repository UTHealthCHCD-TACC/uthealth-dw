-- tx enrollment
 
create or replace view tableau.tx_enrollment_view
as
select data_source, "year", uth_member_id, gender_cd, race_cd, age_derived, state, plan_type, bus_cd, total_enrolled_months
  from tableau.master_enrollment
  where state = 'TX' and data_source in ('mdcd', 'mcrt')
union
select 'cmrc', "year", uth_member_id, gender_cd, race_cd, age_derived, state, plan_type, bus_cd, total_enrolled_months
  from tableau.master_enrollment
  where state = 'TX' 
  and data_source in ('optz', 'truv')
  and bus_cd = 'COM';
 
 
 
-- tx claims
 
create or replace view tableau.tx_claim_view
as
select  b.data_source, b."year" , b.uth_member_id ,total_enrolled_months, gender_cd, age_derived , state, plan_type, bus_cd,
       uth_claim_id , claim_type, total_charge_amount , total_allowed_amount, total_paid_amount
  from tableau.master_claims a
  join tableau.tx_enrollment_view b
    on a.uth_member_id = b.uth_member_id
   and a."year" = b."year";

-- tx covid severity 
drop view if exists tableau.tx_covid_view;  
  
create or replace view tableau.tx_covid_view
as
select data_source, uth_member_id, year, covid_severity, gender_cd, age_derived, plan_type, bus_cd, state
  from tableau.master_enrollment
 where year >= 2020 and state = 'TX' and data_source in ('mdcd', 'mcrt')
union
select 'cmrc', uth_member_id, year, covid_severity, gender_cd, age_derived, plan_type, bus_cd, state
  from tableau.master_enrollment
 where year >= 2020 
  and state = 'TX' 
  and data_source in ('optz', 'truv')
  and bus_cd = 'COM';
 
-- tx conditions   

create or replace view tableau.tx_member_condition_view
as 
select b.*,
aimm, ami, ca, cfib, chf,
ckd, cliv, copd, cysf, dep,
epi, fbm, hemo, hep, hiv,
ihd, lbp, lymp, ms, nicu,
pain, park, pneu, ra, scd,
smi, str, tbi, trans, trau
from tableau.master_enrollment a
join tableau.tx_enrollment_view b
 on a.uth_member_id = b.uth_member_id
and a.year = b.year;

-- tx crg risk
 
 create or replace view tableau.tx_crg_risk_view
as
select b.data_source, 
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
 and a.year = b.year
 where state = 'TX' and b.data_source in ('mdcd', 'mcrt')
union
select 'cmrc',
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
 and a.year = b.year
 where state = 'TX' 
  and b.data_source in ('optz', 'truv')
  and bus_cd = 'COM';

-- granting permissions
 
grant select on tableau.tx_covid_view to uthealth_analyst; 
grant select on tableau.tx_crg_risk_view to uthealth_analyst; 
grant select on tableau.tableau.tx_member_condition_view to uthealth_analyst; 
grant select on tableau.tableau.tx_claim_view to uthealth_analyst; 
grant select on tableau.tableau.tx_enrollment_view to uthealth_analyst; 
