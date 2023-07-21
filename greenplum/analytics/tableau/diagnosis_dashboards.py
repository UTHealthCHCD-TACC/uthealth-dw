import psycopg2
import sys
from tqdm import tqdm
sys.path.append('H:/uth_helpers')
from db_utils import get_dsn




def create_diag_dashboard_table(cursor):
    query = '''
drop table if exists tableau.diag_dashboard;

create table tableau.diag_dashboard
(
    data_source bpchar(4),
    year int,
    members bigint, 
    claims bigint,
    diag_cd text,
    diag_pos text,
    description text
)
with (appendoptimized=true, orientation=column, compresstype=zlib)
;
   '''
    cursor.execute(query)

    query = '''
drop table if exists tableau.tx_diag_dashboard;

create table tableau.tx_diag_dashboard
(
    data_source bpchar(4),
    year int,
    members bigint, 
    claims bigint,
    diag_cd text,
    diag_pos text,
    description text
)
with (appendoptimized=true, orientation=column, compresstype=zlib)
;
   '''
    cursor.execute(query)    

    cursor.execute('''alter table tableau.diag_dashboard owner to uthealth_analyst;''')
    cursor.execute('''alter table tableau.tx_diag_dashboard owner to uthealth_analyst;''')

def fill_diag_dashboard_table(cursor, year):
    query = f'''
insert into tableau.diag_dashboard
select data_source, extract(year from from_date_of_service) as year, count(distinct uth_member_id) as members, count(distinct uth_claim_id) as claims, 
        diag_cd, case when diag_position = 1 then 'Primary' else 'Secondary' end as diag_pos, b.code_description as description 
from (
    select * 
    from data_warehouse.claim_diag 
    where extract(year from from_date_of_service) = {year}
)a
join reference_tables.ref_cms_icd_cm_codes b
on a.diag_cd = b.cd_value 
group by 1,2,5,6,7
;
    '''

    cursor.execute(query)
    cursor.execute('''vacuum analyze tableau.diag_dashboard;''')

def fill_tx_diag_dashboard_table(cursor, year):
    query = f'''
drop table if exists dev.ip_tx_claim_diag_temp;

create table dev.ip_tx_claim_diag_temp(
data_source text,
year int,
uth_member_id bigint,
uth_claim_id bigint,
diag_cd text,
diag_position int)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zlib
)
DISTRIBUTED BY (uth_member_id);

insert into dev.ip_tx_claim_diag_temp
(data_source, year, uth_member_id, uth_claim_id, diag_cd, diag_position)
select b.data_source, b.year, a.uth_member_id, a.uth_claim_id, a.diag_cd, a.diag_position
  from (select * from data_warehouse.claim_diag where extract(year from from_date_of_service) = {year}) a
  join (select * from tableau.tx_claim_view where year = {year}) b
    on a.uth_member_id = b.uth_member_id
   and a.uth_claim_id = b.uth_claim_id
;
    '''

    cursor.execute(query)
    cursor.execute('''vacuum analyze dev.ip_tx_claim_diag_temp;''')

    query = f'''
insert into tableau.tx_diag_dashboard
select a.data_source, a.year, count(distinct a.uth_member_id) as members, count(distinct uth_claim_id) as claims, 
        diag_cd, case when diag_position = 1 then 'Primary' else 'Secondary' end as diag_pos, b.code_description as description 
from dev.ip_tx_claim_diag_temp a
join reference_tables.ref_cms_icd_cm_codes b
on a.diag_cd = b.cd_value 
group by 1,2,5,6,7
;
    '''

    cursor.execute(query)
    cursor.execute('''vacuum analyze tableau.tx_diag_dashboard;''')

if __name__ == '__main__':
    connection = psycopg2.connect(get_dsn()+' keepalives=1 keepalives_idle=30 keepalives_interval=10')
    connection.autocommit = True

    with connection.cursor() as cursor:
        print('Creating Diag Dashboard')
        create_diag_dashboard_table(cursor)
        years = tqdm(range(2014, 2023))
        for year in years:
            years.set_description_str(f'Inserting for diag dasboard year {year}')
            fill_diag_dashboard_table(cursor,year)
            years.set_description_str(f'Inserting for tx diag dasboard year {year}')
            fill_tx_diag_dashboard_table(cursor, year)

    connection.close()