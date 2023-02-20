import pandas as pd
import psycopg2
from tqdm import tqdm
import sys
sys.path.append('H:/uth_helpers/')
from db_utils import get_dsn

def create_tables(cursor, create_master_table=False):
    if create_master_table:
        master_claims = '''drop table if exists dev.ip_master_claims;

    create table dev.ip_master_claims
    (
    data_source text,
    year int,
    uth_member_id int,
    uth_claim_id numeric,
    claim_type text,
    total_charge_amount numeric,
    total_allowed_amount numeric,
    total_paid_amount numeric,
    dx1 text,
    dx2 text,
    dx3 text,
    dx4 text,
    dx5 text,
    dx6 text,
    dx7 text,
    dx8 text,
    dx9 text,
    dx10 text
    )
    with (
            appendonly=true, 
            orientation=column, 
            compresstype=zlib, 
            compresslevel=5 
        )
    distributed by (uth_member_id)
    partition by list(data_source)
    (partition optz values ('optz'),
    partition truv values ('truv'),
    partition mcrt values ('mcrt'),
    partition mcrn values ('mcrn')
    )
    ;

    analyze dev.ip_master_claims;
        '''
        cursor.execute(master_claims)


    temp_dx = '''drop table if exists dev.ip_dx_temp;

create table dev.ip_dx_temp
(
year int,
uth_claim_id numeric,
diag_cd text,
diag_position int
)
with (
		appendonly=true, 
		orientation=column, 
		compresstype=zlib, 
		compresslevel=5 
	 );
	
analyze dev.ip_dx_temp;	
    '''

    
    cursor.execute(temp_dx)

def create_dx_pivot_table(cursor, data_source, year):
    delete_current_rows = 'delete from dev.ip_dx_temp;'
    drop_current_pivot_table = 'drop table if exists dev.ip_pivot_dx_temp;'

    insert_query = f'''insert into dev.ip_dx_temp (year, uth_claim_id, diag_cd, diag_position)	
select extract(year from from_date_of_service), uth_claim_id, diag_cd, diag_position
  from data_warehouse.claim_diag
 where data_source = '{data_source}'
  and diag_position between 1 and 10
  and extract(year from from_date_of_service) = {year}
  ;
    '''

    pivot_table_query = '''select madlib.pivot('dev.ip_dx_temp', 'dev.ip_pivot_dx_temp', 'uth_claim_id', 'diag_position', 'diag_cd', 'max');'''

    cursor.execute(delete_current_rows)
    cursor.execute(drop_current_pivot_table)
    cursor.execute(insert_query)
    cursor.execute(pivot_table_query)

def dx_columns(connection):
    pivot_columns = pd.read_sql('select * from dev.ip_pivot_dx_temp limit 0;', con=connection)

    dx_columns = []

    for col in pivot_columns.columns:
        if col[-1].isdigit():
            dx_columns.append(col)

    n = len(dx_columns)

    insert_columns = ''

    for i in range(n):
        insert_columns += f'dx{i+1}, '

    original_columns = ''

    for col in dx_columns:
        original_columns += f'd1.{col}, '

    return insert_columns[:-2], original_columns[:-2]

def fill_claims_table(connection, data_source, year):
    columns = dx_columns(connection)

    with connection.cursor() as cursor:
        insert_query = f'''
insert into dev.ip_master_claims 
(data_source, year, uth_member_id, uth_claim_id, claim_type,
total_charge_amount, total_allowed_amount, total_paid_amount, 
{columns[0]})
select e.data_source, e.year, e.uth_member_id, h.uth_claim_id, claim_type,
		total_charge_amount, total_allowed_amount, total_paid_amount,
		{columns[1]}
  from (select * from tableau.master_enrollment where data_source = '{data_source}' and year  = {year}) e 
 inner join (select * from data_warehouse.claim_header where data_source = '{data_source}' and year = {year}) h
    on h.uth_member_id = e.uth_member_id
   and h.year = e.year
  left join dev.ip_pivot_dx_temp d1
    on h.uth_claim_id = d1.uth_claim_id;

    analyze dev.ip_master_claims;
    '''

        cursor.execute(insert_query)

def drop_temp_tables(cursor):

    cursor.execute('drop table if exists dev.ip_dx_temp;')
    cursor.execute('drop table if exists dev.ip_pivot_dx_temp;')

if __name__ == '__main__':

    connection = psycopg2.connect(get_dsn())
    connection.autocommit = True

    data_sources = tqdm(['optz', 'truv', 'mcrt', 'mcrn'])
    years = tqdm(range(2012, 2022)) #{'start_year':2012, 'end_year':2021}
    
    print('Creating Tables')
    with connection.cursor() as cursor:
        create_tables(cursor, True)

    for data_source in data_sources:
        data_sources.set_description(data_source)
        for year in years:
            years.set_description(str(year))
            with connection.cursor() as cursor:
                create_dx_pivot_table(cursor, data_source, year)

            fill_claims_table(connection, data_source, year)

    with connection.cursor() as cursor:
        drop_temp_tables(cursor)