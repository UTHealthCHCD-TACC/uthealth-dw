# -*- coding: utf-8 -*-
"""
Created on Sun Jul 10 11:19:11 2022

@author: wcoughlin
"""

import random
import numpy as np 
import string 
import names 
import pandas as pd 
from faker import Faker
from datetime import datetime  
import time
import numpy as np
import uuid
import csv 
import os 


faker = Faker('en_US')

export_path = 'S:\\APCD\\TestData\\'
outfile = export_path + 'data-test_small.csv'


# mixed upper/lower string of specified length 
def randomString (stringLength) :
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(stringLength))

# positive integer of specified length
def randomInt (digits):
    lower = 10**(digits-1)
    upper = 10**digits - 1
    return random.randint(lower, upper)


os.remove(outfile)

t0 = time.time()
with open(outfile, 'a') as csvfile:
    w = csv.writer(csvfile)
    w.writerow(('CDLME001','CDLME002','CDLME003','CDLME004','CDLME005'))
    for i in range(10000000):
        w.writerow(( 
                     randomString(8)       #Data Submitter Code--  varchar 8
                   , randomString(8)       #Payor Code --  varchar 8
                   , randomString(30)       #Plan ID --  varchar 30
                   , randomString(2)       #MemberInsurance/ProductCategory Code--  char 2
                   , random.randint(2020,2021) 
                   
                   ))
        
    


tdelta = time.time() - t0
print(tdelta)