'''
This file is used to look at some (ideally all) of the available record counts for the optum tables.
This allows us to determine the true counts of the optum tables in Greenplum since the updates from optum may provide updates for the claims tables in quarters instead of full years.

Usually just medical, procedure, diagnostic, confinement, rx, lab_result are updated in quarters, so we just need to calculate those values

'''

import pandas as pd
import os
from read_opt_official_counts import create_table_count

def agg_records(update_ym, path, data):

    dfs = {}

    for ym in update_ym:
        files = os.listdir(f'{path}/{ym}/{data}/')

        for file in files:
            if file[:-4] not in dfs:
                dfs[file[:-4]] = pd.read_csv(f'{path}/{ym}/{data}/{file}') 
            else:
                dfs[file[:-4]] = pd.concat([dfs[file[:-4]], pd.read_csv(f'{path}/{ym}/{data}/{file}')])

    
    for key in dfs:
        df_agg = dfs[key].groupby('File Name').max('update_ym')
        df_agg = df_agg.reset_index()
        df_agg = df_agg.set_index(['File Name', 'update_ym'])

        rows = df_agg.index.to_frame(index=False)[['File Name', 'update_ym']]

        dfs[key].merge(rows, how='inner').to_csv(f'{path}/agg_counts/{data}/{key}.csv', index=False)



if __name__ == '__main__':

    updates = ['202210', '202207']

    data = 'optum_dod'

    path = 'transformed_record_counts'

    agg_records(updates, path, data)

    create_table_count(path, data, agg_count=True)