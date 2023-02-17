import pandas as pd
import psycopg2
from datetime import datetime
from tqdm import tqdm
import sys
sys.path.append('H:/uth_helpers/')
from db_utils import get_dsn

def counts(connection, data_source):
    table_names = [('Zip_Medical_', 'medical'),
                    ('zip_Med_Diagnosis_', 'diagnostic'),
                    ('zip_Proc_', 'procedure'),
                    ('zip_confinement_', 'confinement'),
                    ('zip_rx_', 'rx'),
                    ('zip_LabResult_', 'lab_result')]

    quarters = {
                'q1':{'start_date': '01-01', 'end_date': '03-31'},
                'q2':{'start_date': '04-01', 'end_date': '06-30'},
                'q3':{'start_date': '07-01', 'end_date': '09-30'},
                'q4':{'start_date': '10-01', 'end_date': '12-31'}
            }

    enroll_query = f'''
    select null::int as "year", null::text as quarter, 
            count(patid) as row_count, count(distinct patid) as pat_count, null::int as clm_count,
            'mbr_enroll{'_r' if data_source == 'optum_dod' else ''}' as table_source
    from {data_source}.mbr_enroll{'_r' if data_source == 'optum_dod' else ''}
    ;
        '''

    co_enroll_query = f'''
select null::int as "year", null::text as quarter, 
        count(patid) as row_count, count(distinct patid) as pat_count, null::int as clm_count,
        'mbr_co_enroll{'_r' if data_source == 'optum_dod' else ''}' as table_source
  from optum_zip.mbr_co_enroll{'_r' if data_source == 'optum_dod' else ''};
 
        '''
    query_df = pd.read_sql(enroll_query, con=connection)
  
    counts_df = pd.concat([query_df,
                        pd.read_sql(co_enroll_query, con=connection)])

    if data_source == 'optum_dod':
        mbrwdeath_query = '''
    select null::int as "year", null::text as quarter, 
            count(patid) as row_count, count(distinct patid) as pat_count, null::int as clm_count,
            'mbrwdeath' as table_source
    into dev.ip_opt_mbrwdeath_count
    from optum_dod.mbrwdeath
    ;
            '''

        counts_df = pd.concat([query_df,
                        pd.read_sql(mbrwdeath_query, con=connection)])

                        
    years = tqdm(range(2007, 2022))

    for year in years:
        for table in table_names:
            for key, value in quarters.items():
                years.set_description(f'{datetime.now()} Finding counts for table {table[1]} {year} {key}')
                if table[0] == 'zip_confinement_':
                    query = f'''
                    select {year} as "year", '{key}' as quarter, count(patid) as row_count, 
                            count(distinct patid) as pat_count, count(distinct conf_id) as clm_count, '{table[1]}' as table_source
                        from {data_source}.{table[1]}
                        where admit_date between '{year}-{value['start_date']}' and '{year}-{value['end_date']}';
                    '''
                elif table[0] == 'zip_LabResult_':
                    query = f'''
                    select {year} as "year", '{key}' as quarter, count(patid) as row_count,
                            count(distinct patid) as pat_count, count(distinct labclmid) as clm_count, '{table[1]}' as table_source
                        from {data_source}.{table[1]}
                        where fst_dt between '{year}-{value['start_date']}' and '{year}-{value['end_date']}';
                    '''
                elif table[0] == 'zip_rx_':
                    query = f'''
                    select {year} as "year", '{key}' as quarter, count(patid) as row_count, 
                            count(distinct patid) as pat_count, count(distinct clmid) as clm_count, '{table[1]}' as table_source
                        from {data_source}.{table[1]}
                        where fill_dt between '{year}-{value['start_date']}' and '{year}-{value['end_date']}';
                        ''' 
                else: 
                    query = f'''
                    select {year} as "year", '{key}' as quarter, count(patid) as row_count, 
                            count(distinct patid) as pat_count, count(distinct clmid) as clm_count, '{table[1]}' as table_source
                        from {data_source}.{table[1]}
                        where fst_dt between '{year}-{value['start_date']}' and '{year}-{value['end_date']}';
                        '''    
                query_df = pd.read_sql(query, con=connection)
                counts_df = pd.concat([counts_df, query_df])

    # with connection.cursor() as cursor:
    #     date_generated = cursor.execute('''select replace(convert(varchar, getdate(), 1), '/', '');''').fetchone()

    counts_df.to_csv(f'db_counts/gp_{data_source}_counts.csv', index=False)

if __name__ == '__main__':
    connection = psycopg2.connect(get_dsn())

    counts(connection, 'optum_zip')

    connection.close()