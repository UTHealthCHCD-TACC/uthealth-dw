import pandas as pd
import os

def create_count_csv(df, phrase, filepath, update_ym):
    '''
    Reads the given excel file from Optum which contains general information of tables updated.
    Due to the format of this file, we need to implement the logic seen in the create_count_csv function
    to process the file and create seperate dataframes/csv files.
    '''

    # Identify where the input phrase is located and look at 2 rows ahead.
    # This index identifies the columns of the table

    try:
        starting_index = df[df.iloc[:,0] == phrase].index[0] + 2
    except:
        print(phrase + ' Not found')
        return
    # Following logic identifies where the table ends
    empty_cells_bool = df.iloc[starting_index:, 0].isna()
    ending_index = empty_cells_bool[empty_cells_bool == True].index[0] if len(empty_cells_bool[empty_cells_bool == True].index) > 0 else df.shape[0]
    
    # Only get non NaN values at the starting index, meaning that we correctly identify the columns of the table
    columns = df.iloc[starting_index, :].dropna().values

    # Create dataframe representing one of the counts table
    count_df = df.iloc[starting_index+1:ending_index, :columns.shape[0]]
    count_df.columns = columns
    
    count_df.loc[:, 'update_ym'] = count_df.shape[0] * [update_ym]
    count_df.to_csv(filepath, index=False)

def create_table_count(record_directory,data, agg_count=False, update_ym=None):
    '''
    Uses the csv files created in create_count_csv to find the sum of records (rows) by year and table that is available.
    Also determine date range of the data given. Once the sums are calculated, store into a csv file.

    '''
    path = f'{record_directory}/{update_ym}/{data}/' if not agg_count else f'{record_directory}/agg_counts/{data}/'
    filenames = os.listdir(path)

    # Read each csv file in the record directory. This directory should only contain the csv files created in create_count_csv function
    table_count_dfs = []

    for filename in filenames:
        table_count_dfs.append(pd.read_csv(path+filename))

    official_opt_counts = pd.DataFrame(columns=['year', 'quarter', 'table', 'row_count', 'pat_count', 'start_date', 'end_date'])

    # For each the dataframes created from the csv files, identify which table it represents and calculate the sum of records per year and date range for the data
    for df in table_count_dfs:
        table = None
        
        if 'zip5_c' in df.iloc[0,0].lower() or 'dod_c' in df.iloc[0,0].lower():
            table = 'confinement'
        elif 'zip5_lr' in df.iloc[0,0].lower() or 'dod_lr'in df.iloc[0,0].lower():
            table = 'lab_result'
        elif 'zip5_m2' in df.iloc[0,0].lower() or 'dod_m2' in df.iloc[0,0].lower():
            table = 'medical'
        elif 'zip5_proc' in df.iloc[0,0].lower() or 'dod_proc' in df.iloc[0,0].lower():
            table = 'procedure'
        elif 'zip5_diag' in df.iloc[0,0].lower() or 'dod_diag' in df.iloc[0,0].lower():
            table = 'diagnostic'
        elif 'zip5_r' in df.iloc[0,0].lower() or 'dod_r' in df.iloc[0,0].lower():
            table = 'rx'
        elif 'mbr_co_enroll' in df.iloc[0,0].lower():
            # Due to the format given for the enrollment table counts, just need to append record count and date range
            official_opt_counts.loc[len(official_opt_counts.index), :] = [None, None, 'mbr_co_enroll' if data == 'optum_zip' else 'mbr_co_enroll_r', df.loc[0, 'Records'], None, df.loc[0, 'Start Date'], df.loc[0, 'End Date']]
            official_opt_counts.loc[len(official_opt_counts.index), :] = [None, None, 'mbr_enroll' if data == 'optum_zip' else 'mbr_enroll_r' , df.loc[1, 'Records'], None, df.loc[1, 'Start Date'], df.loc[1, 'End Date']]
            continue
        elif 'mbrwdeath' in df.iloc[0,0].lower():
            official_opt_counts.loc[len(official_opt_counts.index), :] = [None, None, 'mbrwdeath', df.loc[0, 'Records'], None, df.loc[0, 'Start Date'], df.loc[0, 'End Date']]
            continue
        elif 'zip5_provider' == df.iloc[0,0].lower() or 'dod_provider' == df.iloc[0,0].lower():
            official_opt_counts.loc[len(official_opt_counts.index), :] = [None, None, 'provider', df.loc[0, 'Records'], None, None, None]
            continue
        elif 'zip5_provider_bridge' == df.iloc[0,0].lower() or 'dod_provider_bridge' == df.iloc[0,0].lower():
            official_opt_counts.loc[len(official_opt_counts.index), :] = [None, None, 'provider_bridge', df.loc[0, 'Records'], None, None, None]
            continue
        elif 'lu' in df.iloc[0,0].lower():
            for row in range(df.shape[0]):
                official_opt_counts.loc[len(official_opt_counts.index), :] = [None, None, df.iloc[row,0], df.iloc[row,1], None, None, None]
            continue
        else:
            continue


        # Identify all years in original dataframe and use it to find aggregated sum
        # df_years = pd.Series([df.iloc[i, 0][-6:-2] for i in range(df.iloc[:, 0].shape[0])])
        # df['year'] = df_years
        
        # for year in df_years.unique():
        #     row_count = df[df['year'] == year]['Records'].sum()
        #     patient_count = df[df['year'] == year]['Distinct Patients'].sum()
        #     # Determine if counts are full year and date range for counts
        #     complete_year = 1 if len(df[df['year'] == year]) == 4 else 0
        #     start_date = df[df['year'] == year]['Start Date'].min() if 'Start Date' in df else None
        #     end_date = df[df['year'] == year]['End Date'].max() if 'End Date' in df else None
        #     official_opt_counts.loc[len(official_opt_counts.index), :] = [year, table, row_count, patient_count, start_date, end_date, complete_year]
    

        for row in range(df.shape[0]):
            year = df.loc[row, 'File Name'][-6:-2]
            quarter = df.loc[row, 'File Name'][-2:]

            official_opt_counts.loc[len(official_opt_counts.index), :] = [year, quarter, table, df.loc[row, 'Records'], df.loc[row, 'Distinct Patients'], 
                                                                            df.loc[row, 'Start Date'] if 'Start Date' in df else None, df.loc[row, 'End Date'] if 'End Date' in df else None ]
    

    if not update_ym and not agg_count:
        official_opt_counts.loc[:, 'update_ym'] = official_opt_counts.shape[0] * [update_ym]
        official_opt_counts.to_csv(f'{record_directory}/{update_ym}/{data}_table_counts.csv', index=False)
    else:
        official_opt_counts.to_csv(f'{record_directory}/agg_counts/{data}_table_counts.csv', index=False)


if __name__ == '__main__':
    data = 'optum_dod'
    optd_file = 'official_record_counts/optum_dod/UTH_DODRv81_Record_Counts_202207.xlsx'
    optz_file = 'official_record_counts/optum_zip/UTH_ZIP5_V81_Record_Counts_202210.xlsx'
    update_ym = '202210'
    df = pd.read_excel(optd_file if data == 'optum_dod' else optz_file)
    
    directory = 'transformed_record_counts'
    print(data)
    create_count_csv(df, 'Update Ranges', f'{directory}/{update_ym}/{data}/{data}_update_ranges.csv', update_ym)
    create_count_csv(df, 'Member Data', f'{directory}/{update_ym}/{data}/{data}_enrollment_count.csv', update_ym)
    create_count_csv(df, 'IP Confinements', f'{directory}/{update_ym}/{data}/{data}_confinement_count.csv', update_ym)
    create_count_csv(df, 'Lab Results', f'{directory}/{update_ym}/{data}/{data}_lab_result_count.csv', update_ym)
    create_count_csv(df, 'Medical Claims', f'{directory}/{update_ym}/{data}/{data}_medical_count.csv', update_ym)
    create_count_csv(df, 'Medical Diagnosis', f'{directory}/{update_ym}/{data}/{data}_diagnosis_count.csv', update_ym)
    create_count_csv(df, 'Medical Procedures', f'{directory}/{update_ym}/{data}/{data}_procedure_count.csv', update_ym)
    create_count_csv(df, 'RX Claims', f'{directory}/{update_ym}/{data}/{data}_rx_count.csv', update_ym)
    create_count_csv(df, 'Provider Data', f'{directory}/{update_ym}/{data}/{data}_provider_count.csv', update_ym)
    create_count_csv(df, 'Provider Bridge Data', f'{directory}/{update_ym}/{data}/{data}_provider_bridge_count.csv', update_ym)
    create_count_csv(df, 'Lookup Tables', f'{directory}/{update_ym}/{data}/{data}_lookup_tables_count.csv', update_ym)
    
    if data == 'optum_dod':
        create_count_csv(df, 'Death Data', f'{directory}/{update_ym}/{data}/{data}_mbrwdeath_count.csv', update_ym)
    
    create_table_count(directory, data, update_ym)