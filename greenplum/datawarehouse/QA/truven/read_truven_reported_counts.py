import os
import pandas as pd
import sys
import psycopg2
from tqdm import tqdm
import re
import xml.etree.ElementTree as ET
sys.path.append('H:/uth_helpers')
from db_utils import get_dsn

def get_worksheet(root, tab_name):
    # Goes through all of the child nodes in the tree 
    # Each child represents a portion of the overall spreadsheet or file.

    # Determines if the "name" of the node is the same as the column we are looking for.
    # This is basically scrolling through the spreadsheet tabs to find the counts for a column,
    # if we open the XML file in excel.
    for child in root:
        name = child.attrib.get('{urn:schemas-microsoft-com:office:spreadsheet}Name')
        if name == tab_name:
            # Move to the body portion of the worksheet
            # need to go further through the tree to retrieve the columns and counts for the table
            body = None

            # child node should have two children - [] and body
            for i in child:
                body = i

            return body
        
    
def get_table_contents(root):
    # Given the node of the worksheet where the table resides,
    # get the columns and rows of the table and convert them to a pandas dataframe
    
    data_attribute = '{urn:schemas-microsoft-com:office:spreadsheet}Data'

    cell_rows = []

    for child in root:
        if 'Row' in child.tag and len(child) > 1:
            cell_rows.append(child)

    columns = []
    rows = []

    # First cell_row should be the name of the columns
    for col in cell_rows[0]:
        columns.append(col.find(data_attribute).text.replace('\n', '')) 

    for row in cell_rows[1:]:
        row_vector = []
        for col in row:
            row_vector.append(col.find(data_attribute).text)

        rows.append(row_vector)

    return pd.DataFrame(rows, columns=columns)
    
def read_xml_files(directory):
    # This function reads the XML file containing the data quality report Truven provides when they send an updated of their data

    file_list = os.listdir(directory)
    file_list = set([file if '.xml' in file else '' for file in file_list] )
    file_list = list(file_list)
    file_list.remove('')
    file_list.remove('redbook.xml')

    counts_df = pd.DataFrame()

    file_list =tqdm(file_list)

    for file in file_list:
        file_list.set_description_str(file)
        table_name = file[:5]

        # While we are just getting the counts for the year column, we can modify this to get frequency for all of the columns that are reported in the XML file
        file_tree = ET.parse(directory+file)
        root = file_tree.getroot()
        table_df = get_table_contents(get_worksheet(root, 'Summary Statistics'))

        table_df['table_name'] = table_name
        table_df['year'] = f'20{file[5:7]}'

        counts_df = pd.concat([counts_df, table_df])#[['YEAR', 'table_name', 'Frequency']]])
        
    # We can change this part to instead create a new table in the database.

    return counts_df.reset_index(drop=True)

def read_txt_files(directory):
    # This function reads all quality reports provided as a text file. 
    # We read all of these files and retrieve the total row count of a table for a given year.
    # Since there can be multiple versions of the data for a given year, we identify the latest version and use the row counts given for that version

    updates_subpaths = os.listdir(directory)
    valid_paths = []

    # Identify all updates available in the directory
    for path in updates_subpaths:
        if 'MScan' in path and ('.zip' not in path and 'HPM' not in path):
            valid_paths.append(path)

    report_files = []


    # Identify full path of each report file
    for path in valid_paths:
        subpaths = os.listdir(directory+path)
        
        data_report_dir = ''
        
        for subpath in subpaths:
            if 'Data Quality Reports' in subpath:
                data_report_dir = subpath
        
        files = os.listdir(f'{directory}{path}/{data_report_dir}')
        
        for file in files:
            if '.txt' in file and 'redbook' not in file:
                report_files.append((f'{directory}{path}/{data_report_dir}/{file}', file))


    # Now we read each of the text files and find the line where it states the number of observations (rows) in the dataset for the table in the given year
    # Due to the format of the report, we need to read each line until we find a line that has a certain pattern.
    # We can modify this portion to read more extensive statistics the report provides, but it requires hard coded logic to identify which lines has the information we need
    rows = []

    report_files = tqdm(report_files)

    for file_path, file in report_files:
        report_files.set_description_str(file)
        with open(file_path, 'r') as f:
            for line in f:
                if 'Dataset' in line:
                    row_count = int(re.search(r'([0-9]{1,3}[,]{0,1}){1,4}', line).group(0).replace(',',''))
                    
                    rows.append((file, file[:5], f'20{file[5:7]}', file[7], row_count))
                    break

    columns = ['File Name', 'Table Name', 'Year', 'Version Number', 'Row Count']

    df = pd.DataFrame(rows, columns=columns)
    
    df_agg = df.groupby(['Table Name', 'Year']).max('Version Number')

    return df_agg.reset_index()

if __name__ == '__main__':
    read_txt = True

    if read_txt:
        count_directory = 'Y:/Data Dictionary/'
        counts_df = read_txt_files(count_directory)
    else:
        # Only latest update have report in an XML file (text files are also provided)
        count_directory = 'Y:/Data Dictionary/MScan Doc for U of Texas 2023-02/5 - Data Quality Reports (Set A)/'
        counts_df = read_xml_files(count_directory)

    counts_df.to_csv('H:/truven_reported_counts.csv', index=False)

    # print('Updating Truven counts table in Greenplum')

    # connection = psycopg2.connect(get_dsn())
    # connection.autocommit = True

    # with connection.cursor() as cursor:
    #     for index in tqdm(counts_df.index):
    #         query = f'''
    #         update dev.ip_truven_counts
    #         set reported_row_count = %s
    #         where year = {counts_df.loc[index, 'YEAR']}
    #         and table_name = '{counts_df.loc[index, 'table_name']}'; '''

    #         cursor.execute(query, (counts_df.loc[index, 'Frequency'],))

    # with connection.cursor() as cursor:  
    #     cursor.execute('update dev.ip_truven_counts set row_count_difference = row_count - reported_row_count;')
    #     cursor.execute('update dev.ip_truven_counts set row_percent_difference = 100. * abs(row_count - reported_row_count) / reported_row_count;')
    #     cursor.execute('update dev.ip_truven_counts set last_updated = now();')
    

    