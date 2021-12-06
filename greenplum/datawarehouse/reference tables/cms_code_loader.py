import io
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

def create_cms_icd_code_tables(cursor):
    cursor.execute("""
    drop table if exists reference_tables.ref_cms_icd_codes;
    create table reference_tables.ref_cms_icd_codes (like reference_tables.ref_cms_codes);
    """)



file_df = pd.read_csv('./cms_codes.csv')
file_df['code_description'] = file_df['code_description'].str.replace('"','')

f = io.StringIO()
file_df.to_csv(f, index=False, header=False, sep='\t')
f.seek(0)

with con.cursor() as cursor:
    create_cms_code_tables(cursor)
    
    cursor.execute('set search_path to reference_tables, public')
    cursor.copy_from(f, 'ref_cms_codes', sep='\t', columns=('cd_type','cd_value','initial_year', 'last_year','code_description'))

    create_cms_icd_code_tables(cursor)
    cursor.execute("""insert into reference_tables.ref_cms_icd_codes select * from 
                      reference_tables.ref_cms_codes where cd_type in ('ICD10-CM', 'ICD9-CM');""")