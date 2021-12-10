import io
import psycopg2
import psycopg2.extras
import pandas as pd
import sys
sys.path.append('H:/uth_helpers/')
from uth_helpers.db_utils import get_dsn


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


def create_cms_icd_cm_code_table(cursor):
    cursor.execute("""
    drop table if exists reference_tables.ref_cms_icd_cm_codes;
    create table reference_tables.ref_cms_icd_cm_codes (like reference_tables.ref_cms_codes);
    insert into reference_tables.ref_cms_icd_cm_codes select * from 
                      reference_tables.ref_cms_codes where cd_type in ('ICD10-CM', 'ICD9-CM');
    """)


def create_cms_icd_pcs_code_table(cursor):
    cursor.execute("""
    drop table if exists reference_tables.ref_cms_icd_pcs_codes;
    create table reference_tables.ref_cms_icd_pcs_codes (like reference_tables.ref_cms_codes);
    insert into reference_tables.ref_cms_icd_pcs_codes select * from 
                      reference_tables.ref_cms_codes where cd_type in ('ICD10-PCS', 'ICD9-PCS');
    """)


if __name__ == '__main__':

    con = psycopg2.connect(get_dsn())
    con.autocommit = True

    file_df = pd.read_csv('./cms_codes.csv')
    file_df['code_description'] = file_df['code_description'].str.replace('"','')

    try:
        with con.cursor() as cursor:
            
            create_cms_code_tables(cursor)
            f = io.StringIO()
            file_df.to_csv(f, index=False, header=False, sep='\t')
            f.seek(0)
            cursor.execute('set search_path to reference_tables, public')
            cursor.copy_from(f, 'ref_cms_codes', sep='\t', columns=('cd_type','cd_value','initial_year', 'last_year','code_description'))

            create_cms_icd_cm_code_table(cursor)
            create_cms_icd_pcs_code_table(cursor)
    except:
        raise

    finally:
        con.close()