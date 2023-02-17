import pandas as pd
import psycopg2
from tqdm import tqdm
import sys
sys.path.append('H:/uth_helpers/')
from db_utils import get_dsn

def create_tables(cursor):
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

    cursor.execute(master_claims)
    cursor.execute(temp_dx)

    print('Created master_claims and dx_temp tables.')

def create_dx_pivot_table(cursor, data_source, start_year, end_year):
    delete_current_rows = 'delete from dev.ip_dx_temp;'

    insert_query = f'''insert into dev.ip_dx_temp (year, uth_claim_id, diag_cd, diag_position)	
select extract(year from from_date_of_service), uth_claim_id, diag_cd, diag_position
  from data_warehouse.claim_diag
 where data_source = '{data_source}'
  and diag_position between 1 and 10
  and extract(year from from_date_of_service) between {start_year} and {end_year}; 

  analyze dev.ip_dx_temp;
    '''

    pivot_table_query = '''select madlib.pivot('dev.ip_dx_temp', 'dev.ip_pivot_dx_temp', 'uth_claim_id', 'diag_position', 'diag_cd', 'max');'''

    cursor.execute(delete_current_rows)
    cursor.execute(insert_query)
    cursor.execute(pivot_table_query)

def fill_claims_table(cursor, data_source, start_year, end_year):
    # Not all claims have 10 or more diagnosis, need to identify how many there were



    insert_query = f'''
insert into tableau.master_claims 
(data_source, year, uth_member_id, uth_claim_id, claim_type,
total_charge_amount, total_allowed_amount, total_paid_amount, 
dx1, dx2, dx3, dx4, dx5, dx6, dx7, dx8, dx9, dx10)
select e.data_source, e.year, e.uth_member_id, h.uth_claim_id, claim_type,
		total_charge_amount, total_allowed_amount, total_paid_amount,
		d1.diag_cd_max_diag_position_1, d1.diag_cd_max_diag_position_2, d1.diag_cd_max_diag_position_3, d1.diag_cd_max_diag_position_4, d1.diag_cd_max_diag_position_5,
		d1.diag_cd_max_diag_position_6, d1.diag_cd_max_diag_position_7, d1.diag_cd_max_diag_position_8, d1.diag_cd_max_diag_position_9, d1.diag_cd_max_diag_position_10
  from (select * from tableau.master_enrollment where data_source = 'optz') e 
  join (select * from data_warehouse.claim_header where data_source = 'optz' and extract(year from from_date_of_service) between 2012 and 2021) h
    on h.uth_member_id = e.uth_member_id
   and h.year = e.year
  join dev.ip_pivot_dx_temp d1
    on h.uth_claim_id = d1.uth_claim_id;
    '''