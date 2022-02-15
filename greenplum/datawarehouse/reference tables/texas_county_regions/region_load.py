import io
import pandas as pd
import psycopg2
import psycopg2.extras
import pyodbc
import sys
sys.path.append('H:/chcd_py/')
from chcd_py.helpers.db_utils import get_dsn

def create_tx_region_gp_table(cursor):
    cursor.execute("""
    drop table if exists reference_tables.ref_tx_county_regions;
    create table reference_tables.ref_tx_county_regions (
        county_number varchar(3),
        county_name varchar(20),
        fips_code int4,
        public_health_region varchar(2),
        health_service_region varchar(10)
    )
    """)

def create_tx_region_ss_table(cursor):
    cursor.execute("""
    drop table if exists htw.dbo.ref_tx_county_regions;
    create table htw.dbo.ref_tx_county_regions (
        county_number varchar(3),
        county_name varchar(20),
        fips_code int,
        public_health_region varchar(2),
        health_service_region varchar(10)
    )
    """)


if __name__ == '__main__':


    tx_region_df = pd.read_excel('greenplum/datawarehouse/reference tables/texas_county_regions/texas_regions.xlsx')
    tx_region_df['county_number'] = tx_region_df['county_number'].astype(str)
    tx_region_df['county_number'] = tx_region_df['county_number'].str.zfill(3)


    try:
        con = psycopg2.connect(get_dsn())
        con.autocommit = True
        with con.cursor() as cursor:
            f = io.StringIO()
            tx_region_df.to_csv(f, index=False, header=False, sep='\t')
            f.seek(0)
            create_tx_region_gp_table(cursor)
            cursor.execute('set search_path to reference_tables, public')
            cursor.copy_from(f, 'ref_tx_county_regions', sep='\t')
    except:
        raise

    finally:
        con.close()

    try:
        con = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=SPCDEDPWPVS1;Database=HTW;Trusted_Connection=yes')

        with con.cursor() as cursor:
            create_tx_region_ss_table(cursor)
            tx_region_df['county_number'] = tx_region_df['county_number'].str.lstrip('0')
        
            tx_region_values = [list(row) for row in tx_region_df.values]
            cursor.executemany("insert into ref_tx_county_regions values (?, ?, ?, ? ,?)", tx_region_values)
    except:
        raise
    finally:
        con.close()