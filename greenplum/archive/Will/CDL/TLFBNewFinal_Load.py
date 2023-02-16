# -*- coding: utf-8 -*-
"""
Created on Thu Aug 11 13:32:24 2022

@author: wcoughlin
"""

import pyodbc 
import csv 



work_path = 'H:\\Homes\\'

data_file = work_path + 'HOMESTLFBFinal_DATA_LoadFile_20220809.csv'


cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=SPCDEDPWPVS1;"
                      "Database=Homes;"
                      "Trusted_Connection=yes;")


cursor = cnxn.cursor() 

cursor.execute ('TRUNCATE TABLE  RAW.Homes_TLFB_Final_20220809;')
cursor.commit()


with open (data_file, 'r') as f:
    reader = csv.reader(f)
    columns = next(reader) 
    query = 'insert into RAW.Homes_TLFB_Final_20220809 ({0}) values ({1})'
    query = query.format(','.join(columns), ','.join('?' * len(columns)))
    for data in reader:
        data[:] = (elem[:99] for elem in data)
        cursor.execute(query,data)
        cursor.commit()
        


cnxn.close()
