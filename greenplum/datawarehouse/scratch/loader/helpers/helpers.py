import io
import os
import time
import zipfile
import requests
from functools import wraps
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def url_zip_outputter(output_dir, url, dir_name=None):
    '''downloads the zip file from a url and unzips file into the output_dir'''
    if dir_name is not None:
        zip_output_dir = os.path.join(output_dir, dir_name)
    else:
        zip_output_dir = output_dir
    if os.path.exists(zip_output_dir):
        print('ZIP already extracted')
    else:
        r = requests.get(url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(zip_output_dir)
        print(f'Zip Extracted: {zip_output_dir}')


def explain_query(cursor, query):

    if not query.startswith('explain'):
        query = 'explain ' + query
    cursor.execute(query)
    results = cursor.fetchall()
    explain_output = "\n".join([i[0] for i in results])
    return explain_output


def timer_profile(fn):
    @wraps(fn)
    def inner(*args, **kwargs):
        fn_kwargs_str = ', '.join(f'{k}={v}' for k, v in kwargs.items())
        # print(f'\n{fn.__name__}({fn_kwargs_str})')

        t = time.perf_counter()
        retval = fn(*args, **kwargs)
        elapsed = time.perf_counter() - t
        # print(f'(Result: {retval[0][0]:,}, Time Elapsed: {elapsed:0.4})')
        return (retval, elapsed)
    return inner


def query_comparison_profiler(con, table_col_list, profile_function, arg_list, xlabels=None):
    '''
    con: database connection
    table_col_list: list of tables or columns that will be compared
    profile_function: function that executes a query; this must return a time elapsed in seconds
    arg_list: list of arguments that will be ran in the query
    '''
    count = 1
    total_run_count = len(table_col_list)*len(arg_list)
    results_dict = {}

    query_output = []
    for ind_var in table_col_list:
        results_dict[ind_var] = []
        for arg in arg_list:
            # print(f"\nRun#: {count}/{total_run_count}", end='')
            count += 1
            with con.cursor() as cursor:
                output = profile_function(cursor, ind_var=ind_var, arg=arg)
                results_dict[ind_var].append(output[1])
                query_output.append(output[0][0][0])
                print('*', end='')

    results_df = pd.DataFrame(results_dict)

    results_melt_df = pd.melt(results_df.reset_index(), id_vars=['index'],
                              value_vars=table_col_list)
    results_melt_df.columns = ['index', 'table', 'seconds']

    plt.rcParams['figure.figsize'] = [10, 5]
    g = sns.boxplot(x='table', y='seconds', data=results_melt_df)
    g.set_xlabel(None)
    if xlabels is not None:
        g.set_xticklabels(xlabels)
        results_df.columns = xlabels
    results_df['args'] = arg_list
    results_df['query_results'] = query_output[:len(query_output)//len(table_col_list)]

    return results_df
