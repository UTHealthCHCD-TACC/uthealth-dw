import pandas as pd
import psycopg2
import sys
sys.path.append('H:/uth_helpers/')
from db_utils import get_dsn

def delete_old_data(cursor, data_source):
    query = f'''delete from tableau.master_enrollment where data_source = '{data_source}';'''

    cursor.execute(query)

    return cursor.rowcount

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
		trau, asth, dem, diab, htn, opi, tob
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
  where year = 2020
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
;
    '''

    cursor.execute(query)

    rowcount = cursor.rowcount

    cursor.execute('vacuum analyze tableau.master_enrollment;')

    return rowcount

def qa(connection):
    # Comparing With Member Enrollment Yearly Table
    query = '''
with dw as (
    select data_source, year, count(distinct uth_member_id) dw_member_count, count(*) dw_row_count
    from data_warehouse.member_enrollment_yearly
    where year >= 2014
    group by 1,2
),
tableau as (
    select data_source, year, count(distinct uth_member_id) tableau_member_count, count(*) tableau_row_count
    from tableau.master_enrollment
    group by 1,2
)
select a.*, b.dw_member_count, b.dw_row_count
from tableau a
join dw b
on a.data_source = b.data_source
and a.year = b.year
order by 1,2
;
    '''

    df = pd.read_sql(query, con=connection)

    df['member_check'] = df['dw_member_count'] == df['tableau_member_count']
    df['row_check'] = df['dw_row_count'] == df['tableau_row_count']

    print('Enrollment Table Mismatch')
    print(df[~df['member_check'] | ~df['row_check']])


    # Comparing With Conditions Member Enrollment Yearly Table
    query = '''
with dw as (
    select data_source, year, count(distinct uth_member_id) dw_member_count, count(*) dw_row_count
    from data_warehouse.conditions_member_enrollment_yearly
    where year >= 2014
    group by 1,2
),
tableau as (
    select data_source, year, count(distinct uth_member_id) tableau_member_count, count(*) tableau_row_count
    from tableau.master_enrollment
    group by 1,2
)
select a.*, b.dw_member_count, b.dw_row_count
from tableau a
join dw b
on a.data_source = b.data_source
and a.year = b.year
order by 1,2
;
    '''

    df = pd.read_sql(query, con=connection)

    df['member_check'] = df['dw_member_count'] == df['tableau_member_count']
    df['row_check'] = df['dw_row_count'] == df['tableau_row_count']

    print('Conditions Enrollment Table Mismatch')
    print(df[~df['member_check'] | ~df['row_check']])

    # Comparing With Covid Severity Table
    query = '''
with dw as (
    select data_source, year, count(distinct uth_member_id) dw_member_count, count(*) dw_row_count
    from data_warehouse.covid_severity
    where year >= 2020
    group by 1,2
),
tableau as (
    select data_source, year, count(distinct uth_member_id) tableau_member_count, count(*) tableau_row_count
    from tableau.master_enrollment
    where year >= 2020
    and covid_severity is not null
    group by 1,2
)
select a.*, b.dw_member_count, b.dw_row_count
from tableau a
join dw b
on a.data_source = b.data_source
and a.year = b.year
order by 1,2
;
    '''

    df = pd.read_sql(query, con=connection)

    df['member_check'] = df['dw_member_count'] == df['tableau_member_count']
    df['row_check'] = df['dw_row_count'] == df['tableau_row_count']

    print('COVID Severity Table Mismatch')
    print(df[~df['member_check'] | ~df['row_check']])

    # Comparing With CRG Table
    query = '''
with dw as (
    select data_source, crg_year, count(distinct uth_member_id) dw_member_count, count(*) dw_row_count
    from data_warehouse.crg_risk
    where crg_year >= 2014
    group by 1,2
),
tableau as (
    select data_source, year, count(distinct uth_member_id) tableau_member_count, count(*) tableau_row_count
    from tableau.master_enrollment
    group by 1,2
)
select a.*, b.dw_member_count, b.dw_row_count
from tableau a
join dw b
on a.data_source = b.data_source
and a.year = b.crg_year
order by 1,2
;
    '''

    df = pd.read_sql(query, con=connection)

    df['member_check'] = df['dw_member_count'] == df['tableau_member_count']
    df['row_check'] = df['dw_row_count'] == df['tableau_row_count']

    print('CRG Table Mismatch')
    print(df[~df['member_check'] | ~df['row_check']])

if __name__ == '__main__':
    connection = psycopg2.connect(get_dsn()+' keepalives=1 keepalives_idle=30 keepalives_interval=10')
    connection.autocommit = True

    data_sources = [
        # 'optz',
        # 'optd',
        # 'truc',
        # 'trum',
        # 'mcrt',
        # 'mcrn', 
        # 'mdcd',
        # 'mhtw',
        # 'mcpp'
        ]

    for data_source in data_sources:
        with connection.cursor() as cursor:
            print(f'Number of {data_source} Rows deleted: ', delete_old_data(cursor, data_source))
            print(f'Number of {data_source} Rows inserted: ', insert_new_data(cursor, data_source))

    print('Starting QA for Master Enrollment Table')
    qa(connection)

    connection.close()