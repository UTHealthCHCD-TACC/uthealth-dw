import pandas as pd
import psycopg2
import os
import sys
from datetime import datetime
sys.path.append('H:/uth_helpers/')
from db_utils import get_dsn, io_copy_from
import crg_helpers

def check_directory(directory):
    # Checks whether folder for the given year exists
    # If it does not exists, create folder and sub folders

    print(datetime.now().strftime('%A, %d %B %Y %I:%M %p'), 'Checking if required folder exists')
    if not os.path.exists(directory):
        print('Creating the directory ', directory)
        os.mkdir(directory)
    else:
        print(directory, ' already exists')

    if not os.path.exists(directory+'input'):
        print('Creating the directory ', directory+'input')
        os.mkdir(directory+'input')
    else:
        print(directory+'input', ' already exists')

    if not os.path.exists(directory+'output'):
        print('Creating the directory ', directory+'output')
        os.mkdir(directory+'output')
    else:
        print(directory+'output', ' already exists')

    if not os.path.exists(directory+'error'):
        print('Creating the directory ', directory+'error')
        os.mkdir(directory+'error')
    else:
        print(directory+'error', ' already exists')

    if not os.path.exists(directory+'report'):
        print('Creating the directory ', directory+'report')
        os.mkdir(directory+'report')
    else:
        print(directory+'report', ' already exists')

def run_command(input_template, output_template, directory, data_source, year, start_age, end_age, use_fiscal_year=False):
    # Identify start and end dates based on whether using calendar year or fiscal year
    start_date = f'0101{year}' if not use_fiscal_year else f'0901{year-1}'
    end_date = f'1231{year}' if not use_fiscal_year else f'0831{year}'

    command  = 'C:\\ProgramData\\3mhis\\cgs\\cgs_console.exe'
    command += f''' -input {directory}\\input\\{data_source}_input_crg_{'cy' if not use_fiscal_year else 'fy'}_{year}_{start_age}_{end_age}.csv'''
    command += f''' -input_template {input_template}'''
    command += f''' -upload {directory}\\output\\{data_source}_output_crg_{'cy' if not use_fiscal_year else 'fy'}_{year}_{start_age}_{end_age}.csv'''
    command += f''' -upload_template {output_template}'''
    command += f''' -report {directory}\\report\\{data_source}_report_crg_{'cy' if not use_fiscal_year else 'fy'}_{year}_{start_age}_{end_age}.txt'''
    command += f''' -error_log {directory}\\error\\{data_source}_error_crg_{'cy' if not use_fiscal_year else 'fy'}_{year}_{start_age}_{end_age}.txt'''
    command += f''' -schedule off -grouper 32200'''
    command += f''' -analysis_start_date {start_date}'''
    command += f''' -analysis_end_date {end_date}'''

    os.system(command)

if __name__ == '__main__':
    # Requirements
    input_template = 'C:\\3mhis\\cgs\\templates\\crg_in.2022.3.1.dic'
    output_template = 'C:\\3mhis\\cgs\\templates\\crg_out.2022.3.1.dic'
    crg_files_path = 'Y:\\_3M\\CRG\\'
    
    start_year = 2022
    end_year = 2022
    use_fiscal_year = True
    use_src_ids = True

    age_windows = [
                    {'start_age': 0, 'end_age': 15},
                    {'start_age': 16, 'end_age': 30},
                    {'start_age': 31, 'end_age': 45},
                    {'start_age': 46, 'end_age': 55},
                    {'start_age': 56, 'end_age': 65},
                    {'start_age': 66, 'end_age': 75},
                    {'start_age': 76, 'end_age': 85},
                    {'start_age': 86, 'end_age': 95},
                    {'start_age': 96, 'end_age': 200}
                ]

    data_sources = [
                    'mdcd',
                    # 'mcpp', 
                    # 'mhtw', 
                    # 'mcrt',
                    # 'mcrn',
                    # 'optz',
                    # 'optd',
                    # 'truc', 
                    # 'trum',
                    ]

    for data_source in data_sources:
        connection = psycopg2.connect(get_dsn())
        connection.autocommit = True
        # If it is for calendar year, create output table, typically used when crg are generated for DW
        with connection.cursor() as cursor:
            if not use_fiscal_year:
                crg_helpers.create_crg_table(cursor, 'dev', f'ip_{data_source}_crg_risk')

        for year in range(start_year, end_year+1):
            check_directory(crg_files_path+data_source+f'''\\{'' if not use_fiscal_year else 'FY'}{year}\\''')

            # CRG Process
            if connection.closed:
                connection = psycopg2.connect(get_dsn())
                connection.autocommit = True
            # Generate subtables, input table, output table
            with connection.cursor() as cursor:
                print(datetime.now().strftime('%A, %d %B %Y %I:%M %p'), 'Generating Input Table')
                crg_helpers.generate_crg_input_table(cursor, data_source, year, use_fiscal_year, use_src_ids)

            connection.close()

            # Generate input files based on age groups/windows and run 3M
            for age_window in age_windows:

                connection = psycopg2.connect(get_dsn())
                with connection.cursor() as cursor:
                    crg_helpers.crg_input_file_batch_export(cursor, year, data_source, age_window['start_age'], age_window['end_age'], crg_files_path+data_source+f'''\\{'' if not use_fiscal_year else 'FY'}{year}\\input\\''', use_fiscal_year)
                connection.close()

                print(datetime.now().strftime('%A, %d %B %Y %I:%M %p'), 'Running 3M Software')
                run_command(input_template, output_template, crg_files_path, data_source, year, age_window['start_age'], age_window['end_age'], use_fiscal_year)

                # Note that this is only for when CRG scores are generated for the data warehouse
                if not use_fiscal_year:
                    print(datetime.now().strftime('%A, %d %B %Y %I:%M %p'), 'Importing 3M results to database')
                    connection = psycopg2.connect(get_dsn())
                    connection.autocommit = True

                    file = crg_files_path+data_source+f'''\\{'' if not use_fiscal_year else 'FY'}{year}\\output\\{data_source}_output_crg_{'cy' if not use_fiscal_year else 'fy'}_{year}_{age_window['start_age']}_{age_window['end_age']}.csv'''
                    crg_df = crg_helpers.crg_read_csv(file, data_source, year)
                    print(f'Number of row inserted to ip_{data_source}_crg_risk: ', io_copy_from(connection, crg_df, 'dev', f'ip_{data_source}_crg_risk'))
                    
                    connection.close()

    if not connection.closed:
        connection.close()