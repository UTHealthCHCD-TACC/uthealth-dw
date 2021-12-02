import numpy as np
import pandas as pd
import pyodbc
from datetime import datetime


transfer_discharge_cds = ['02', '05', '65', '82', '85', '88', '93', '94','30']

def transfer_dt_adjuster(df):
    #adjusts one day for transfer codes and creates the transfer adjusted discharge date
    df.loc[:, 'transfer_cd'] = False
    df.loc[df['discharge_status'].isin(transfer_discharge_cds), 'transfer_cd']=True
    df.loc[:, 'transfer_adj_discharge_date'] = None
    df.loc[df['transfer_cd']==False,
           'transfer_adj_discharge_date'] = df.loc[df['transfer_cd']==False,'discharge_date']
    df.loc[df['transfer_cd']==True,
               'transfer_adj_discharge_date'] = df.loc[df['transfer_cd']==True,'discharge_date'] + pd.Timedelta(days=1)
    df['transfer_adj_discharge_date'] = pd.to_datetime(df['transfer_adj_discharge_date'])
    return df

def enc_identifier(df, is_sorted=False, drop_intermediate_cols=False):
    if not is_sorted:
        df = df.sort_values(['uth_member_id', 'admit_date', 'discharge_date'], 
                            ascending=[1,1,0])
    #checks if there is an overlap between admit_date and the last discharge_date
    #if there is no overlap between the dates it becomes a new event
    df['greatest_discharge_date'] = df.groupby(['uth_member_id'])['transfer_adj_discharge_date'].cummax()
    df['lag_discharge_date'] = df.groupby(['uth_member_id'])['greatest_discharge_date'].shift(1)
    df['enc_id'] = df['admit_date']>df['lag_discharge_date']
    df['enc_id'] = df.groupby('uth_member_id')['enc_id'].cumsum()
    
    #finds maximum and minimum dates for the encounter
    #be sure to not use the adjusted dschrg for max encounter dschrg
    df['enc_admit_date'] = df.groupby(['uth_member_id','enc_id'])['admit_date'].transform(min)
    df['enc_discharge_date'] = df.groupby(['uth_member_id','enc_id'])['discharge_date'].transform(max)
    if drop_intermediate_cols:
        df.drop(['greatest_discharge_date', 'lag_discharge_date'], axis=1, inplace=True)
    return df

def admit_id_output(df):
    admit_id_series = (df['uth_member_id'].astype('str')+'-'+
                       df['enc_id'].astype('str').str.zfill(3)+'-'+
                      (df['admit_date'].dt.year).astype('str'))
    return admit_id_series

def encounter_row_counter(df):
    if 'enc_row_count' in df.columns:
        print('previously calculated; drop column to repeat')
        return df
    else:
        clm_count = df.groupby(level=[0,1])[['uth_claim_id']].count()
        clm_count = clm_count.rename(columns={'uth_claim_id': 'enc_row_count'})
        df = pd.merge(df, clm_count, how='left', right_index=True, left_index=True)
        return df
    
def admit_encounter_status(df):
    #those that only have one row will have the status of that row as the encounter status
    clm_segment_1 = df.loc[df['enc_row_count']==1, ['discharge_status']]
    clm_segment_1['enc_discharge_status'] = clm_segment_1.loc[:, 'discharge_status']
    #those that have more we need to look at which code takes precedence
    clm_segment_gt1 = df.loc[(df['enc_row_count']>1)&
                             (df['discharge_date']==df['enc_discharge_date']),
                             ['discharge_status', 'bill_type']]
    #death codes 
    death_status = ['20','21','40','41','42']

    discharge_status_vals = []
    clm_gp = clm_segment_gt1.groupby(level=[0,1])

    for n, group in clm_gp:
        #get unique statuses on end date
        discharge_statuses = group['discharge_status'].unique()
        #if there is only one type of status that is the status they have
        if len(discharge_statuses) == 1:
            discharge_status_vals.append({'uth_member_id':n[0], 'enc_id':n[1], 
                                          'enc_discharge_status':discharge_statuses[0]})
        else:
            if np.isin(discharge_statuses, death_status).any():
                discharge_status_vals.append({'uth_member_id':n[0],'enc_id':n[1],
                                              'enc_discharge_status':discharge_statuses.max()})
            else:
                discharge_statuses.sort()
                #take the minimum other than '00'
                if '00' in discharge_statuses:
                    if len(discharge_statuses) == 2 and 'NA' in discharge_statuses:
                        status = discharge_statuses[0]
                    else: 
                        status = discharge_statuses[1]
                else:
                    status = discharge_statuses[0]
                discharge_status_vals.append({'uth_member_id':n[0], 'enc_id':n[1], 'enc_discharge_status':status})
    
    #creates a dataframe from the encounter status, if there was no values in the
    #clm_segment_gt1 then there is no data needed 
    if len(discharge_status_vals)>0:
        enc_discharge_status_gt1 = pd.DataFrame(discharge_status_vals)
        enc_discharge_status_gt1 = enc_discharge_status_gt1.set_index(['uth_member_id','enc_id'])
        enc_statuses = pd.concat([clm_segment_1[['enc_discharge_status']],
                                  enc_discharge_status_gt1])
    else:
        enc_statuses = clm_segment_1[['enc_discharge_status']]
        
    return enc_statuses

if __name__ =='__main__':
    source_table = 'dev.gm_dw_ip_window_step_2'
    output_csv = './dw_ip_window.csv'
    
    con = pyodbc.connect('DSN=PostgreSQL35W')
    cur = con.cursor()
    cur.execute(f'''select distinct data_source, pat_group  
                from {source_table} order by data_source, pat_group;''')
    results = cur.fetchall()
    cur.close()
    
    try:
        #creates new file when true, appends to file when false
        reset = True
        for pat_group in results:
            print(pat_group)
            start = datetime.now()
            sql_string = f'''select * from {source_table} where
                             data_source = '{pat_group[0]}' and
                             pat_group = {pat_group[1]} ;''';
            clm_df = pd.read_sql(sql_string, con=con, 
                                 parse_dates=['admit_date', 'discharge_date'])
            end = datetime.now()
            time_to_read = end-start
            print(f'''time to read: {time_to_read}''')
            clm_df = clm_df.sort_values(['uth_member_id', 'admit_date', 'discharge_date'], ascending=[1,1,0])
            
            #remove this later
            clm_df['discharge_status'] = clm_df['discharge_status'].fillna('NA')
            
            clm_df = transfer_dt_adjuster(clm_df)
            clm_df = enc_identifier(clm_df, is_sorted=True)
            clm_df.loc[:, 'admit_id'] = admit_id_output(clm_df)
            clm_df = clm_df.set_index(['uth_member_id','enc_id'])
            clm_df = encounter_row_counter(clm_df)
            enc_statuses = admit_encounter_status(clm_df)
            clm_df = pd.merge(clm_df, enc_statuses, left_index=True, right_index=True, how='left')
            
            final_ip_group = clm_df.groupby(level=[0,1]).agg(admit_date=('admit_date','min'),
                                                             discharge_date=("discharge_date", "max"),
                                                             enc_discharge_status=("enc_discharge_status", "first"),
                                                             admit_id=('admit_id','first'),
                                                             data_source=('data_source','first'))
            print(final_ip_group.shape[0])
            if reset:
                final_ip_group.to_csv(output_csv)
                reset = False
            else:
                final_ip_group.to_csv(output_csv, mode='a', header=False)
                
    except:
        raise
    finally:
        con.close()

# \copy dev.gm_dw_ip_admit(uth_member_id, enc_id, admit_date, discharge_date, enc_discharge_status, admit_id, data_source) from 'H:\Notebooks\IP_Windows\optum_dw_ip_window.csv' delimiter ',' csv header;