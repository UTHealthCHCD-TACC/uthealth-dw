import pyodbc
import pandas as pd
import datetime
from tqdm import tqdm

def counts(connection):
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


  enroll_query = '''
          select  null as [year], null as quarter, count(patid) as row_count, 
                  count(distinct patid) as pat_count, null as clm_count, 'mbr_enroll' as table_source,
                  replace(convert(varchar, getdate(), 1), '/', '') as date_generated
            from OPT_ZIP.dbo.zip5_mbr_enroll
            ;
  '''

  co_enroll_query = '''
          select  null as [year], null as quarter, count(patid) as row_count, 
                  count(distinct patid) as pat_count, null as clm_count, 'mbr_co_enroll' as table_source,
                  replace(convert(varchar, getdate(), 1), '/', '') as date_generated
            from OPT_ZIP.dbo.zip5_mbr_co_enroll
            ;
  '''

  query_df = pd.read_sql(enroll_query, con=connection)
  
  counts_df = pd.concat([query_df,
                        pd.read_sql(co_enroll_query, con=connection)])

  years = tqdm(range(2007, 2022))

  for year in years:
    for table in table_names:
      for key, value in quarters.items():
        years.set_description(f'{datetime.datetime.now()} Finding counts for table {table[0]}{year} {key}')
        if table[0] == 'zip_confinement_':
          query = f'''
          select {year} as [year], '{key}' as quarter, count(patid) as row_count, 
                  count(distinct patid) as pat_count, count(distinct conf_id) as clm_count, '{table[1]}' as table_source,
                  replace(convert(varchar, getdate(), 1), '/', '') as date_generated
            from OPT_ZIP.dbo.{table[0]}{year}
            where admit_date between '{year}-{value['start_date']}' and '{year}-{value['end_date']}'
          '''
        elif table[0] == 'zip_LabResult_':
          query = f'''
          select {year} as [year], '{key}' as quarter, count(patid) as row_count,
                  count(distinct patid) as pat_count, count(distinct labclmid) as clm_count, '{table[1]}' as table_source,
                  replace(convert(varchar, getdate(), 1), '/', '') as date_generated
            from OPT_ZIP.dbo.{table[0]}{year}
            where fst_dt between '{year}-{value['start_date']}' and '{year}-{value['end_date']}'
          '''
        elif table[0] == 'zip_rx_':
          query = f'''
          select {year} as [year], '{key}' as quarter, count(patid) as row_count, 
                  count(distinct patid) as pat_count, count(distinct clmid) as clm_count, '{table[1]}' as table_source,
                  replace(convert(varchar, getdate(), 1), '/', '') as date_generated
            from OPT_ZIP.dbo.{table[0]}{year}
            where fill_dt between '{year}-{value['start_date']}' and '{year}-{value['end_date']}'
            ''' 
        else: 
          query = f'''
          select {year} as [year], '{key}' as quarter, count(patid) as row_count, 
                  count(distinct patid) as pat_count, count(distinct clmid) as clm_count, '{table[1]}' as table_source,
                  replace(convert(varchar, getdate(), 1), '/', '') as date_generated
            from OPT_ZIP.dbo.{table[0]}{year}
            where fst_dt between '{year}-{value['start_date']}' and '{year}-{value['end_date']}'
            '''    
        query_df = pd.read_sql(query, con=connection)
        counts_df = pd.concat([counts_df, query_df])

  with connection.cursor() as cursor:
    date_generated = cursor.execute('''select replace(convert(varchar, getdate(), 1), '/', '');''').fetchone()

  counts_df.to_csv(f'db_counts/local_optz_counts_{date_generated[0]}.csv', index=False)

if __name__ == '__main__':
    connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=spcdedpwpvs1;Trusted_Connection=yes;')

    counts(connection)

    connection.close()