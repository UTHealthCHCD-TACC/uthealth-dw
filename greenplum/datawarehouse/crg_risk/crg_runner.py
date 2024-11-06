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
    # These folders are for storing the input files and all 3M related files

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
    command += f''' -input {directory}{data_source}\\{'' if not use_fiscal_year else 'FY'}{year}\\input\\{data_source}_input_crg_{'cy' if not use_fiscal_year else 'fy'}_{year}_{start_age}_{end_age}.csv'''
    command += f''' -input_template {input_template}'''
    command += f''' -upload {directory}{data_source}\\{'' if not use_fiscal_year else 'FY'}{year}\\output\\{data_source}_output_crg_{'cy' if not use_fiscal_year else 'fy'}_{year}_{start_age}_{end_age}.csv'''
    command += f''' -upload_template {output_template}'''
    command += f''' -report {directory}{data_source}\\{'' if not use_fiscal_year else 'FY'}{year}\\report\\{data_source}_report_crg_{'cy' if not use_fiscal_year else 'fy'}_{year}_{start_age}_{end_age}.txt'''
    command += f''' -error_log {directory}{data_source}\\{'' if not use_fiscal_year else 'FY'}{year}\\error\\{data_source}_error_crg_{'cy' if not use_fiscal_year else 'fy'}_{year}_{start_age}_{end_age}.txt'''
    command += f''' -schedule off -grouper 32200'''
    command += f''' -analysis_start_date {start_date}'''
    command += f''' -analysis_end_date {end_date}'''

    os.system(command)

if __name__ == '__main__':
    # Requirements
    input_template = 'C:\\3mhis\\cgs\\templates\\crg_in.2022.3.1.dic'
    output_template = 'C:\\3mhis\\cgs\\templates\\crg_out.2022.3.1.dic'
    crg_files_path = 'Y:\\_3M\\CRG\\' #'Y:\\_3M\\CRG\\'
    
    start_year = 2020
    end_year = 2020
    
    use_fiscal_year = False
    use_src_ids = False
    recreate_intermediate_tables = False
    create_data_source_crg_risk_table = False

    age_windows = [
					# {'start_age': 0, 'end_age': 10},
					# {'start_age': 11, 'end_age': 20},
					# {'start_age': 21, 'end_age': 30},
					# {'start_age': 31, 'end_age': 40},
					# {'start_age': 41, 'end_age': 50},
					# {'start_age': 51, 'end_age': 60},
					# {'start_age': 61, 'end_age': 70},
					# {'start_age': 71, 'end_age': 80},
					{'start_age': 81, 'end_age': 90},
					{'start_age': 91, 'end_age': 200}
                ]

    data_sources = [
                    # 'mdcd',
                    # 'mcpp', 
                    # 'mhtw', 
                    'mcrt',
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
        # Also check if we want to create the crg risk table for the given data_source,
        # typically this flag is for if multiple instances of 3M is being run for different years concurrently 
        # so that the overall crg_risk table for the data source is not deleted everytime the script is used
        with connection.cursor() as cursor:
            if create_data_source_crg_risk_table:
                crg_helpers.create_crg_table(cursor, 'dev', f'ip_{data_source}_crg_risk')

        for year in range(start_year, end_year+1):
            check_directory(crg_files_path+data_source+f'''\\{'' if not use_fiscal_year else 'FY'}{year}\\''')

            #Generate subtables not including enrollment table
            if connection.closed:
                connection = psycopg2.connect(get_dsn())
                connection.autocommit = True

            if recreate_intermediate_tables:
                with connection.cursor() as cursor:
                    print(datetime.now().strftime('%A, %d %B %Y %I:%M %p'), 'Generating Subtables')
                    crg_helpers.generate_crg_input_table(cursor, data_source, year, use_fiscal_year=use_fiscal_year)

            connection.close()

            # Generate input files based on age groups/windows and run 3M
            for age_window in age_windows:
                if connection.closed:
                    connection = psycopg2.connect(get_dsn())
                    connection.autocommit = True

                with connection.cursor() as cursor:
                    print(datetime.now().strftime('%A, %d %B %Y %I:%M %p'), f'''Generating Enrollment and Input Table for ages {age_window['start_age']} to {age_window['end_age']}''')
                    crg_helpers.generate_crg_input_table(cursor, data_source, year, age_window['start_age'], age_window['end_age'], use_fiscal_year, use_src_ids)
                    
                    print(datetime.now().strftime('%A, %d %B %Y %I:%M %p'), 'Exporting Input Table To a CSV File')
                    crg_helpers.crg_input_file_batch_export(cursor, year, data_source, age_window['start_age'], age_window['end_age'], crg_files_path+data_source+f'''\\{'' if not use_fiscal_year else 'FY'}{year}\\input\\''', use_fiscal_year)
                connection.close()

                print(datetime.now().strftime('%A, %d %B %Y %I:%M %p'), 'Running 3M Software')
                # Automatically runs 3M software once input file is generated as a CSV file.
                run_command(input_template, output_template, crg_files_path, data_source, year, age_window['start_age'], age_window['end_age'], use_fiscal_year)

                # Note that this is only for when CRG scores are generated for the data warehouse
                if not use_fiscal_year:
                    print(datetime.now().strftime('%A, %d %B %Y %I:%M %p'), 'Importing 3M results to database')
                    connection = psycopg2.connect(get_dsn())
                    connection.autocommit = True

                    file = crg_files_path+data_source+f'''\\{'' if not use_fiscal_year else 'FY'}{year}\\output\\{data_source}_output_crg_{'cy' if not use_fiscal_year else 'fy'}_{year}_{age_window['start_age']}_{age_window['end_age']}.csv'''
                    crg_df = crg_helpers.crg_read_csv(file, data_source, year)
                    if crg_df is not None:
                        print(f'Number of row inserted to ip_{data_source}_crg_risk: ', io_copy_from(connection, crg_df, 'dev', f'ip_{data_source}_crg_risk'))
                    
                    connection.close()

            if connection.closed:
                connection = psycopg2.connect(get_dsn())
                connection.autocommit = True

            # with connection.cursor() as cursor:
            #     crg_helpers.drop_intermediate_tables(cursor, data_source, year, use_fiscal_year)
    
    if not connection.closed:
        connection.close()