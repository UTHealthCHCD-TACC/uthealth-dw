import pandas as pd
import psycopg2
from tqdm import tqdm
import sys
sys.path.append('H:/uth_helpers/')
from db_utils import get_dsn

def create_temp_table(cursor, data_source):
    temp_table = f'''drop table if exists dev.ip_master_temp_{data_source};

create table dev.ip_master_temp_{data_source}
(like tableau.master_claims including defaults)
with (
		appendonly=true, 
		orientation=column, 
		compresstype=zlib, 
		compresslevel=5 
	 );
	
analyze dev.ip_master_temp_{data_source};	
    '''

    
    cursor.execute(temp_table)

def create_dx_table(cursor, data_source):
    temp_dx = f'''drop table if exists dev.ip_dx_temp_{data_source};

create table dev.ip_dx_temp_{data_source}
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
	
analyze dev.ip_dx_temp_{data_source};	
    '''

    
    cursor.execute(temp_dx)

def create_dx_pivot_table(cursor, data_source, year):
    delete_current_rows = f'delete from dev.ip_dx_temp_{data_source};'
    drop_current_pivot_table = f'drop table if exists dev.ip_pivot_dx_temp_{data_source};'

    insert_query = f'''insert into dev.ip_dx_temp_{data_source} (year, uth_claim_id, diag_cd, diag_position)	
select extract(year from from_date_of_service), uth_claim_id, diag_cd, diag_position
  from data_warehouse.claim_diag
 where data_source = '{data_source}'
  and diag_position between 1 and 10
  and extract(year from from_date_of_service) = {year}
  ;
    '''

    pivot_table_query = f'''select madlib.pivot('dev.ip_dx_temp_{data_source}', 'dev.ip_pivot_dx_temp_{data_source}', 'uth_claim_id', 'diag_position', 'diag_cd', 'max');'''

    cursor.execute(delete_current_rows)
    cursor.execute(drop_current_pivot_table)
    cursor.execute(insert_query)
    cursor.execute(pivot_table_query)

def dx_columns(connection):
    pivot_columns = pd.read_sql(f'select * from dev.ip_pivot_dx_temp_{data_source} limit 0;', con=connection)

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
insert into dev.ip_master_temp_{data_source}
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
  left join dev.ip_pivot_dx_temp_{data_source} d1
    on h.uth_claim_id = d1.uth_claim_id;

    analyze dev.ip_master_temp_{data_source};
    '''

        cursor.execute(insert_query)

def swap_partitions(cursor, data_source):
    swap_query = f'''
    alter table tableau.master_claims
    exchange partition {data_source}
    with table dev.ip_master_temp_{data_source}
    ;
    '''

    cursor.execute(swap_query)

def drop_temp_tables(cursor, data_source):

    cursor.execute(f'drop table if exists dev.ip_dx_temp_{data_source};')
    cursor.execute(f'drop table if exists dev.ip_pivot_dx_temp_{data_source};')
    # cursor.execute(f'drop table if exists dev.ip_master_temp_{data_source};')

if __name__ == '__main__':

    connection = psycopg2.connect(get_dsn())
    connection.autocommit = True
    # 'optz', 'truv', 'mcrt', 'mcrn', 'mdcd'
    data_sources = ['truv']
    years = tqdm(range(2014, 2022)) #{'start_year':2012, 'end_year':2021}
    
    print('Creating Tables')
    

    for data_source in data_sources:
        with connection.cursor() as cursor:
            create_temp_table(cursor, data_source)
            create_dx_table(cursor, data_source)
        print(data_source)
        for year in years:
            years.set_description(str(year))
            with connection.cursor() as cursor:
                create_dx_pivot_table(cursor, data_source, year)

            fill_claims_table(connection, data_source, year)

        with connection.cursor() as cursor:
            swap_partitions(cursor, data_source)
            drop_temp_tables(cursor, data_source)