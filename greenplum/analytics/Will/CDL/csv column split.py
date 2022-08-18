# -*- coding: utf-8 -*-
"""
Created on Thu Aug 11 13:32:24 2022

@author: wcoughlin
"""

import pandas as pd 
import pyodbc 
import csv 



work_path = 'H:\\Homes\\'

data_file = work_path + 'HOMESNewFinal_DATA_2022-08-09_1015.csv'
outfile1 = work_path + 'HOMESNewFinal_Split1.csv'
outfile2 = work_path + 'HOMESNewFinal_Split2.csv'
outfile3 = work_path + 'HOMESNewFinal_Split3.csv'



df = pd.read_csv(data_file)


final_df1 = df.iloc[:,0:750] 
final_df2 = df.iloc[:,751:1500]
final_df3 = df.iloc[:,1501:]

final_df1.index.name = 'row_sequence'
final_df2.index.name = 'row_sequence'
final_df3.index.name = 'row_sequence'



cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=SPCDEDPWPVS1;"
                      "Database=Homes;"
                      "Trusted_Connection=yes;")



create_sql_1 = 'CREATE TABLE RAW.HomesNewFinal_1 ( '

with open (outfile1, 'r') as f:
    reader = csv.reader(f)
    columns = next(reader) 
    print (columns)
    cur = cnxn.cursor()
    for c in columns:
        create_sql_1 += c + ' varchar(250), '
    
    create_sql_1 += 'filler char(1) );'
    
    print(create_sql_1)
        
    cur.execute(create_sql_1)
    cur.commit()



#final_df1.to_sql (name = 'Table1', con = cnxn)



#final_df1.to_csv(outfile1)
#final_df2.to_csv(outfile2)
#final_df3.to_csv(outfile3)




