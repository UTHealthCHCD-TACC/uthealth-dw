# -*- coding: utf-8 -*-
"""
Created on Mon Jul 11 07:51:53 2022

@author: wcoughlin
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Jun 21 11:17:07 2022

@author: wcoughlin
"""

import random
import string 
from faker import Faker
import time
import os 
import gc 

work_path = 'S:\\APCD\\TestData\\Work\\'

export_path = 'S:\\APCD\\TestData\\'
outfile = export_path + 'Provider_2GB.txt'

chunksize = 500_000

fake = Faker('en_US')

try:
    os.remove(outfile)
except OSError:
    pass 

# mixed upper/lower string of specified length 
def randomString (stringLength) :
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(stringLength))

# positive integer of specified length
def randomInt (digits):
    lower = 10**(digits-1)
    upper = 10**digits - 1
    return random.randint(lower, upper)


def date_generator(v_start, v_end):
    v_date = str(fake.date_between(start_date = v_start, end_date = v_end) ).replace('-','') 
    return v_date



header_line = ['HD|1111111|PAYER11|SAMPLE FILE|PV|201907|202010|T|This is a sample PROVIDER file\n']

trailer_line = ['TR|1111111|PAYER11|SAMPLE FILE|PV|20211007||2376']

column_names = ['CDLPV001|CDLPV002|CDLPV003|CDLPV004|CDLPV005|CDLPV006|CDLPV007|CDLPV008|'\
                'CDLPV009|CDLPV010|CDLPV011|CDLPV012|CDLPV013|CDLPV014|CDLPV015|CDLPV016|'\
                'CDLPV017|CDLPV018|CDLPV019|CDLPV020|CDLPV021|CDLPV022|CDLPV023|CDLPV024|'\
                'CDLPV025|CDLPV026|CDLPV027|CDLPV028|CDLPVXXX|CDLPV899\n']
 
    
with open(outfile, 'a') as provFile:
    provFile.writelines(header_line) 
    provFile.writelines(column_names)    

loop_counter = 5 

while loop_counter > 0:    
    t1 = time.time()       
    
    data = [ 
                   [ '1111111' for i in range(chunksize)]
                , [ 'PAYER11' for i in range(chunksize)]
                , [ randomString(30) for i in range(chunksize)]
                , [ randomString(30) for i in range(chunksize)]
                , [ randomString(10) for i in range(chunksize)]
                , [ randomString(1) for i in range(chunksize)]
                , [ randomString(10) for i in range(chunksize)]
                , [ randomString(12) for i in range(chunksize)]
                , [ randomString(15) for i in range(chunksize)]
                , [ randomString(35) for i in range(chunksize)]
                , [ randomString(25) for i in range(chunksize)]
                , [ randomString(60) for i in range(chunksize)]
                , [ randomString(10) for i in range(chunksize)]
                , [ randomString(55) for i in range(chunksize)]
                , [ randomString(30) for i in range(chunksize)]
                , [ randomString(2) for i in range(chunksize)]
                , [ randomString(9) for i in range(chunksize)]
                , [ randomString(5) for i in range(chunksize)]
                , [ randomString(2) for i in range(chunksize)]
                , [ randomString(10) for i in range(chunksize)]
                , [ randomString(10) for i in range(chunksize)]
                , [ randomString(10) for i in range(chunksize)]
                , [ randomString(30) for i in range(chunksize)]
                , [ randomString(30) for i in range(chunksize)]
                , [ randomString(10) for i in range(chunksize)]
                , [ randomString(10) for i in range(chunksize)]
                , [ randomString(10) for i in range(chunksize)]
                , [ randomString(10) for i in range(chunksize)]
                , [ randomString(1) for i in range(chunksize)]
                , [ 'PV' for i in range(chunksize)]
    ]        
            
            
    rows = ['%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|'\
            '%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n' % row for row in zip(*data)]
    
    tdelta1 = time.time() - t1
    print('Prov - data gen', tdelta1)
    
    
    
    t0 = time.time()
    
    with open(outfile, 'a') as provFile:
        provFile.writelines(rows)
        
    tdelta0 = time.time() - t0
    print('Prov - data load', tdelta0)     
    
    loop_counter = loop_counter - 1       
    data = []
    gc.collect()    
    
    
with open(outfile, 'a') as provFile:
    provFile.writelines(trailer_line)    
    
    