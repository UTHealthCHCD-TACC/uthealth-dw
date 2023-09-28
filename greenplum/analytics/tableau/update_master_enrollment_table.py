import pandas as pd
import psycopg2
import sys
sys.path.append('H:/uth_helpers/')
from db_utils import get_dsn

def delete_old_data(cursor, data_source):
    query = f'''delete from tableau.master_enrollment where data_source = '{data_source}';'''

    cursor.execute(query)

def insert_new_data(cursor, data_source):
    query = f'''
with enrl as(
select data_source, year, uth_member_id, gender_cd, race_cd, age_derived, 
		state, plan_type, bus_cd, total_enrolled_months
from data_warehouse.member_enrollment_yearly a 
where a.year >= 2014
and data_source = '{data_source}'
),
cond  as (
select data_source, "year" , uth_member_id,
      aimm, ami, ca, cfib, chf, ckd, cliv, copd, 
		cysf, dep, epi, fbm, hemo, hep, hiv, 
		ihd, lbp, lymp, ms, nicu, pain, park, 
		pneu, ra, scd, smi, str, tbi, trans, 
		trau asth, dem, diab, htn, opi, tob
from data_warehouse.conditions_member_enrollment_yearly 
where year >= 2014
),
crg as (
select
	cr.data_source,
	cr.uth_member_id,
	cr.crg_year,
	cr.crg,
	concat(left(crg, 1), right(crg, 1)) as crg_abbreviated
from data_warehouse.crg_risk cr
where cr.crg_year >= 2014
),
covid as (
select *
  from data_warehouse.covid_severity
)
insert into tableau.master_enrollment
select e.*, c.aimm, c.ami, c.ca, c.cfib, c.chf, c.ckd, c.cliv, c.copd, 
		c.cysf, c.dep, c.epi, c.fbm, c.hemo, c.hep, c.hiv, 
		c.ihd, c.lbp, c.lymp, c.ms, c.nicu, c.pain, c.park, 
		c.pneu, c.ra, c.scd, c.smi, c.str, c.tbi, c.trans, 
		c.trau, c.asth, c.dem, c.diab, c.htn, c.opi, c.tob, 
		cr.crg, cr.crg_abbreviated,
		cs.severity as covid_severity
 from enrl e
 join cond c
   on e.uth_member_id = c.uth_member_id
  and e.year = c.year
  and e.data_source = c.data_source
 left join crg cr
   on e.uth_member_id = cr.uth_member_id
  and e.year = cr.crg_year
  and e.data_source = cr.data_source
 left join covid cs
   on e.uth_member_id = cs.uth_member_id
  and e.year = cs.year
  and e.data_source = cs.data_source
;'''