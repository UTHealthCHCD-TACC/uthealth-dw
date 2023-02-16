# -*- coding: utf-8 -*-
"""
Created on Fri Aug 12 09:32:28 2022

@author: wcoughlin
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Aug 11 13:32:24 2022

@author: wcoughlin
"""

import pandas as pd 
import pyodbc 
import csv 



work_path = 'H:\\Homes\\'

data_file = work_path + 'GPRANewFinal_DATA_2022-08-09_1020.csv'

load_file = work_path + 'GPRANewFinal_LoadFile_20220809.csv'


df = pd.read_csv(data_file)


final_df1 = df.iloc[:,:] 


final_df1.index.name = 'row_sequence'


final_df1.to_csv(load_file)


cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=SPCDEDPWPVS1;"
                      "Database=Homes;"
                      "Trusted_Connection=yes;")



create_sql = 'CREATE TABLE RAW.GPRANewFinal_20220809 ( '

with open (load_file, 'r') as f:
    reader = csv.reader(f)
    columns = next(reader) 
    cur = cnxn.cursor()
    for c in columns:
        create_sql += c + ' varchar(100), '
    
    create_sql += 'filler char(1) );'
    
    
    cur.execute(create_sql)
    cur.commit()



cnxn.close()




