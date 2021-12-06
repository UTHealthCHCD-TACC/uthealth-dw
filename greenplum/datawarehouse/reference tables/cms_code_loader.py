import psycopg2
import psycopg2.extras
import pandas as pd

con = psycopg2.connect(database='uthealth', host='greenplum01.cdorral.tacc.utexas.edu', 
                       user='user', port=5432, password="password")

con.autocommit = True

def create_cms_code_tables(cursor):
    cursor.execute("""
    drop table if exists reference_tables.ref_cms_codes;
    create table reference_tables.ref_cms_codes (
        cd_type varchar(20),
        cd_value varchar(10),
        initial_year int4,
        last_year int4,
        code_description text
    )
    distributed randomly;
    """)

file_df = pd.read_csv('./cms_codes.csv')
file_df['code_description'] = file_df['code_description'].str.replace('"','')

with con.cursor() as cursor:
    create_cms_code_tables(cursor)
    tuples = [tuple(x) for x in file_df.to_numpy()]
    cols = ','.join(list(file_df.columns))
    query = "insert into %s(%s) values %%s" % ("reference_tables.ref_cms_codes", cols)
    psycopg2.extras.execute_values(cursor, query, tuples, page_size=5000)
    