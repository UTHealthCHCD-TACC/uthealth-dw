import pandas as pd
import pyodbc
import psycopg2
import sys
sys.path.append('H:/uth_helpers/')
from db_utils import get_dsn

def download_ndc_table():
    print('Exporting Table to a CSV File')
    connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=spcdedpwpvs1;Trusted_Connection=yes;')

    df = pd.read_sql('select * from [REF].dbo.NDC_TIER_MAP', con=connection)
    df.to_csv('H:/NDC_TIER_MAP.csv', index=False)

    print(f'New NDC Table rows: {df.shape[0]}')

    connection.close()

def gp_upload_ndc_table():
    print('Importing CSV NDC Table to Greenplum')
    connection = psycopg2.connect(get_dsn())
    connection.autocommit = True

    with connection.cursor() as cursor, open('H:/NDC_TIER_MAP.csv', 'r') as file:
        query = '''
        drop table if exists dev.ip_ndc_tier_map_old;

        select *
        into dev.ip_ndc_tier_map_old
        from reference_tables.ndc_tier_map;
        '''

        cursor.execute(query)

        print(f'Old GP NDC rowcount: {cursor.rowcount}')

        cursor.execute('delete from reference_tables.ndc_tier_map')

        cursor.copy_expert(f'copy reference_tables.ndc_tier_map from stdin with CSV HEADER', file)

        cursor.execute('select count(*) as row_count from reference_tables.ndc_tier_map;')

        new_rowcount = cursor.fetchall()[0][0]

        print(f'New GP NDC rowcount: {new_rowcount}')

    connection.close()

if __name__ == '__main__':

    download_ndc_table()
    gp_upload_ndc_table()