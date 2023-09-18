from datetime import datetime
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
import sys
sys.path.append('H:/')
from uth_helpers.db_utils import get_dsn, io_copy_from


def transfer_dt_adjuster(df):
    '''
    Parameters
    ----------
    df: pandas dataframe with acute inpatient discharge dates

    Returns
    -------
    df: pandas dateframe

        returns dataframe with adjusted transfer dates
    '''
    transfer_discharge_cds = ['02', '05', '65', '82', '85', '88', '93', '94', '30']

    df.loc[:, 'transfer_cd'] = False
    df.loc[df['discharge_status'].isin(transfer_discharge_cds), 'transfer_cd'] = True
    df.loc[:, 'transfer_adj_discharge_date'] = None
    df.loc[df['transfer_cd'] == False,
           'transfer_adj_discharge_date'] = df.loc[df['transfer_cd'] == False, 'discharge_date']
    df.loc[df['transfer_cd'] == True,
           'transfer_adj_discharge_date'] = df.loc[df['transfer_cd'] == True, 'discharge_date'] + pd.Timedelta(days=1)
    df['transfer_adj_discharge_date'] = pd.to_datetime(df['transfer_adj_discharge_date'])
    return df


def enc_identifier(df, is_sorted=False, drop_intermediate_cols=False):
    '''
    Parameters
    ----------
    df : pandas dataframe
        with the patient id, admit_date and discharge date
    is_sorted : Booleen, optional
        Dataframe must be sorted for operation to work correctly,
        if sorted is false, the sort in function is bipassed
    drop_intermediate_cols : Boolean, optional
        Deletes intermediate calculated columns if true

    Returns
    -------
    df : pandas dataframe
        adds columns that determine encounters time period.
        The encounter time period is between by 'enc_admit_date' and
        'enc_discharge_date'
    '''

    if not is_sorted:
        df = df.sort_values(['uth_member_id', 'admit_date', 'discharge_date'],
                            ascending=[1, 1, 0])
    # checks if there is an overlap between admit_date and the last discharge_date
    # if there is no overlap between the dates it becomes a new event
    df['greatest_discharge_date'] = df.groupby(['uth_member_id'])['transfer_adj_discharge_date'].cummax()
    df['lag_discharge_date'] = df.groupby(['uth_member_id'])['greatest_discharge_date'].shift(1)
    df['enc_id'] = df['admit_date'] > df['lag_discharge_date']
    df['enc_id'] = df.groupby('uth_member_id')['enc_id'].cumsum()

    # finds maximum and minimum dates for the encounter
    # be sure to not use the adjusted dschrg for max encounter dschrg
    df['enc_admit_date'] = df.groupby(['uth_member_id', 'enc_id'])['admit_date'].transform(min)
    df['enc_discharge_date'] = df.groupby(['uth_member_id', 'enc_id'])['discharge_date'].transform(max)
    if drop_intermediate_cols:
        df.drop(['greatest_discharge_date', 'lag_discharge_date'], axis=1, inplace=True)
    return df


def admit_id_output(df):
    '''
    Parameters
    ----------
    df: pandas dataframe
        Must contain the encounter id and admit date

    Returns
    -------
    admit_id_series: pandas series
    The series is a unique identifier for the admit
    '''
    admit_id_series = (df['uth_member_id'].astype('str') + '-' +
                       df['enc_id'].astype('str').str.zfill(3) + '-' +
                      (df['admit_date'].dt.year).astype('str'))
    return admit_id_series


def encounter_row_counter(df):
    '''
    Parameters
    ----------
    df : pandas dataframe
        df with uth_member_id, enc_id as index

    Returns
    -------
    df : pandas data frame
        returns altered df that has a row count for each encounter

    This is used to make the function admit_encounter_status more efficient,
    identifying the encounters that only have one status.
    '''
    if 'enc_row_count' in df.columns:
        print('previously calculated; drop column to repeat')
        return df
    else:
        clm_count = df.groupby(level=[0, 1])[['uth_claim_id']].count()
        clm_count = clm_count.rename(columns={'uth_claim_id': 'enc_row_count'})
        df = pd.merge(df, clm_count, how='left',
                      right_index=True, left_index=True)
        return df


def admit_encounter_status(df):
    '''
    Parameters
    ----------
    df : pandas dataframe
        dataframe has the encounter_row_count, and discharge status

    Returns
    -------
    enc_statuses : pandas dataframe
        dataframe that contains the encounter's discharge status

    '''
    # those that only have one row will have the status of that row as the encounter status
    clm_segment_1 = df.loc[df['enc_row_count'] == 1, ['discharge_status']]
    clm_segment_1['enc_discharge_status'] = clm_segment_1.loc[:, 'discharge_status']
    # those that have more we need to look at which code takes precedence
    clm_segment_gt1 = df.loc[(df['enc_row_count'] > 1) &
                             (df['discharge_date'] == df['enc_discharge_date']),
                             ['discharge_status', 'bill_type']]
    # death codes
    death_status = ['20', '21', '40', '41', '42']

    discharge_status_vals = []
    clm_gp = clm_segment_gt1.groupby(level=[0, 1])

    for n, group in clm_gp:
        # get unique statuses on end date
        discharge_statuses = group['discharge_status'].unique()
        # if there is only one type of status that is the status they have
        if len(discharge_statuses) == 1:
            discharge_status_vals.append({'uth_member_id': n[0],
                                          'enc_id': n[1],
                                          'enc_discharge_status': discharge_statuses[0]})
        else:
            if np.isin(discharge_statuses, death_status).any():
                discharge_status_vals.append({'uth_member_id': n[0],
                                              'enc_id': n[1],
                                              'enc_discharge_status': discharge_statuses.max()})
            else:
                discharge_statuses.sort()
                # take the minimum other than '00'
                if '00' in discharge_statuses:
                    if len(discharge_statuses) == 2 and 'NA' in discharge_statuses:
                        status = discharge_statuses[0]
                    else:
                        status = discharge_statuses[1]
                else:
                    status = discharge_statuses[0]
                discharge_status_vals.append({'uth_member_id': n[0],
                                              'enc_id': n[1],
                                              'enc_discharge_status': status})

    # creates a dataframe from the encounter status,
    # if there was no values in the
    # clm_segment_gt1 then there is no data needed
    if len(discharge_status_vals) > 0:
        enc_discharge_status_gt1 = pd.DataFrame(discharge_status_vals)
        enc_discharge_status_gt1 = enc_discharge_status_gt1.set_index(['uth_member_id', 'enc_id'])
        enc_statuses = pd.concat([clm_segment_1[['enc_discharge_status']],
                                  enc_discharge_status_gt1])
    else:
        enc_statuses = clm_segment_1[['enc_discharge_status']]
    return enc_statuses


def ip_window_wrapper(clm_df):
    '''Performs the steps into a singular definition'''
    clm_df = clm_df.sort_values(['uth_member_id', 'admit_date', 'discharge_date'], ascending=[1, 1, 0])

    # fills null discharge statuses...
    clm_df['discharge_status'] = clm_df['discharge_status'].fillna('NA')

    clm_df = transfer_dt_adjuster(clm_df)
    clm_df = enc_identifier(clm_df, is_sorted=True)
    clm_df.loc[:, 'admit_id'] = admit_id_output(clm_df)
    clm_df = clm_df.set_index(['uth_member_id', 'enc_id'])
    clm_df = encounter_row_counter(clm_df)
    enc_statuses = admit_encounter_status(clm_df)
    clm_df = pd.merge(clm_df, enc_statuses, left_index=True, right_index=True, how='left')

    final_ip_group = clm_df.groupby(level=[0, 1]).agg(admit_date=('admit_date', 'min'),
                                                      discharge_date=("discharge_date", "max"),
                                                      enc_discharge_status=("enc_discharge_status", "first"),
                                                      admit_id=('admit_id', 'first'),
                                                      data_source=('data_source', 'first'),
                                                      min_bill_type=('bill_type', 'min'),
                                                      max_bill_type=('bill_type', 'max'),
                                                      member_id_src=('member_id_src', 'first'))
    
    final_ip_group['missing_terminal_status'] = ~((final_ip_group['min_bill_type'].isin(['111', '114', '117'])) & (final_ip_group['max_bill_type'].isin(['111', '114', '117'])))
    final_ip_group['missing_terminal_status_117'] = (final_ip_group['min_bill_type'] != final_ip_group['max_bill_type']) & (final_ip_group['max_bill_type'].isin(['117']) | final_ip_group['min_bill_type'].isin(['117']))
    print(final_ip_group.shape[0])

    # set in column order for load

    columns = [
        'data_source', 
        'uth_member_id', 
        'enc_id', 
        'admit_date',
        'discharge_date', 
        'enc_discharge_status',
        'admit_id',
        'missing_terminal_status',
        'missing_terminal_status_117',
        'member_id_src'
    ]

    final_ip_group = final_ip_group.reset_index()
    final_ip_group = final_ip_group[columns]
    
    return final_ip_group


if __name__ == '__main__':
    schema = 'dev'
    source_table = 'gm_dw_ip_window_step_2'
    output_table = 'gm_dw_ip_admit'

    con = psycopg2.connect(get_dsn())
    con.autocommit = True

    with con.cursor() as cursor:
        cur = con.cursor()
        cur.execute(f'''select distinct data_source, pat_group
                    from {schema}.{source_table} order by data_source, pat_group;''')
        results = cur.fetchall()
        print(results)
 
    try:
        for pat_group in results:
            print(pat_group)
            sql_string = f'''select * from {schema}.{source_table} where
                             data_source = '{pat_group[0]}' and
                             pat_group = {pat_group[1]};'''
            clm_df = pd.read_sql(sql_string, con=con,
                                 parse_dates=['admit_date', 'discharge_date'])

            final_ip_group = ip_window_wrapper(clm_df)
            final_ip_group.loc[:, 'insert_ts'] = datetime.now()
            io_copy_from(con, final_ip_group, schema, output_table)

    except:
        raise
    finally:
        con.close()
