# -*- coding: utf-8 -*-
"""
Created on Mon Jul 11 13:56:02 2022

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
outfile = export_path + 'Pharmacy_07112022.txt'

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

gender_list = ['M','M','M','M','F','F','F','F','U']


def date_generator(v_start, v_end):
    v_date = str(fake.date_between(start_date = v_start, end_date = v_end) ).replace('-','') 
    return v_date



header_line = ['HD|1111111|PAYER11|SAMPLE FILE|PC|201907|202010|T|This is a sample PHARM file\n']

trailer_line = ['TR|1111111|PAYER11|SAMPLE FILE|PC|20211007||2376']

column_names = ['CDLPC001|CDLPC002|CDLPC003|CDLPC004|CDLPC005|CDLPC006|CDLPC007|CDLPC008|CDLPC009|'\
                'CDLPC010|CDLPC011|CDLPC012|CDLPC013|CDLPC014|CDLPC015|CDLPC016|CDLPC017|CDLPC018|'\
                'CDLPC019|CDLPC020|CDLPC021|CDLPC022|CDLPC023|CDLPC024|CDLPC025|CDLPC026|CDLPC027|'\
                'CDLPC028|CDLPC029|CDLPC030|CDLPC031|CDLPC032|CDLPC033|CDLPC034|CDLPC035|CDLPC036|'\
                'CDLPC037|CDLPC038|CDLPC039|CDLPC040|CDLPC041|CDLPC042|CDLPC043|CDLPC044|CDLPC045|'\
                'CDLPC046|CDLPC047|CDLPC048|CDLPC049|CDLPC050|CDLPC051|CDLPC052|CDLPC053|CDLPC054|'\
                'CDLPC055|CDLPC056|CDLPC057|CDLPC058|CDLPC059|CDLPC060|CDLPC061|CDLPC062|CDLPC063|'\
                'CDLPC064|CDLPC065|CDLPC066|CDLPC067|CDLPC068|CDLPC069|CDLPC070|CDLPC071|CDLPCXXX|CDLPC899\n']

with open(outfile, 'a') as pharmFile:
    pharmFile.writelines(header_line) 
    pharmFile.writelines(column_names)    

loop_counter = 10 

while loop_counter > 0:
    
    t1 = time.time()       
    
    data = [ 
        [ '1111111' for i in range(chunksize)]    #Data Submitter Code
        ,[ 'PAYER11' for i in range(chunksize)]    #Payor Code 
        ,[ randomString(30) for i in range(chunksize)]    #Plan ID 
        ,[ randomString(2) for i in range(chunksize)]    #Member Insurance/ Product Category Code
        ,[ randomString(35) for i in range(chunksize)]    #Payor Claim Control Number
        ,[ randomInt(4) for i in range(chunksize)]    #Line Counter 
        ,[ randomInt(4) for i in range(chunksize)]    #Version Number 
        ,[ randomString(35) for i in range(chunksize)]    #Cross Reference Claims ID
        ,[ randomString(50) for i in range(chunksize)]    #Insured Group or Policy Number
        ,[ randomString(4) for i in range(chunksize)]    #Medicaid AID Category 
        ,[ randomString(9) for i in range(chunksize)]    #Subscriber Social Security Number
        ,[ randomString(80) for i in range(chunksize)]    #Plan Specific Contract Number
        ,[ randomString(60) for i in range(chunksize)]    #Subscriber Last Name 
        ,[ randomString(35) for i in range(chunksize)]    #Subscriber First Name 
        ,[ randomString(20) for i in range(chunksize)]    #Sequence Number 
        ,[ randomString(9) for i in range(chunksize)]    #Member Social Security Number
        ,[ randomString(2) for i in range(chunksize)]    #Individual Relationship Code
        ,[ randomString(1) for i in range(chunksize)]    #Member Gender 
        ,[ randomString(8) for i in range(chunksize)]    #Member Date of Birth 
        ,[ randomString(60) for i in range(chunksize)]    #Member Last Name 
        ,[ randomString(35) for i in range(chunksize)]    #Member First Name 
        ,[ randomString(9) for i in range(chunksize)]    #Member ZIP Code 
        ,[ randomString(8) for i in range(chunksize)]    #Date Prescription Filled
        ,[ randomString(8) for i in range(chunksize)]    #Paid Date 
        ,[ randomString(11) for i in range(chunksize)]    #Drug Code 
        ,[ randomString(2) for i in range(chunksize)]    #New Prescription or Refill
        ,[ randomString(2) for i in range(chunksize)]    #Generic Drug Indicator 
        ,[ randomString(1) for i in range(chunksize)]    #Dispensed as Written Code
        ,[ randomString(1) for i in range(chunksize)]    #Compound Drug Indicator
        ,[ randomString(128) for i in range(chunksize)]    #Compound Drug Name or Compound Drug Ingredient List
        ,[ randomString(1) for i in range(chunksize)]    #Formulary Indicator 
        ,[ randomInt(10 ) for i in range(chunksize)]    #Quantity Dispensed 
        ,[ randomInt(3) for i in range(chunksize)]    #Days’ Supply 
        ,[ randomString(3) for i in range(chunksize)]    #Drug Unit of Measure 
        ,[ randomString(20) for i in range(chunksize)]    #Prescription Number 
        ,[ randomInt(10) for i in range(chunksize)]    #Charge Amount 
        ,[ randomInt(10) for i in range(chunksize)]    #Plan Paid Amount 
        ,[ randomInt(12) for i in range(chunksize)]    #Allowed Amount 
        ,[ randomInt(12) for i in range(chunksize)]    #Sales Tax Amount 
        ,[ randomInt(10) for i in range(chunksize)]    #Ingredient Cost/List Price
        ,[ randomInt(10) for i in range(chunksize)]    #Postage Amount Claimed
        ,[ randomInt(10) for i in range(chunksize)]    #Dispensing Fee 
        ,[ randomInt(10) for i in range(chunksize)]    #Copay Amount 
        ,[ randomInt(10) for i in range(chunksize)]    #Coinsurance Amount 
        ,[ randomInt(10) for i in range(chunksize)]    #Deductible Amount 
        ,[ randomInt(12) for i in range(chunksize)]    #COB/TPL Amount 
        ,[ randomInt(10) for i in range(chunksize)]    #Other Insurance Paid Amount
        ,[ randomInt(12) for i in range(chunksize)]    #Member Self-pay Amount
        ,[ randomString(2) for i in range(chunksize)]    #Payment Arrangement Type Flag
        ,[ randomString(30) for i in range(chunksize)]    #Prescribing Physician ID 
        ,[ randomString(10) for i in range(chunksize)]    #Prescribing Physician NPI 
        ,[ randomString(25) for i in range(chunksize)]    #Prescribing Physician First Name
        ,[ randomString(60) for i in range(chunksize)]    #Prescribing Physician Last Name
        ,[ randomString(7) for i in range(chunksize)]    #Pharmacy NCPDP Number
        ,[ randomString(30) for i in range(chunksize)]    #Pharmacy ID 
        ,[ randomString(10) for i in range(chunksize)]    #Pharmacy Tax ID Number
        ,[ randomString(10) for i in range(chunksize)]    #Pharmacy NPI 
        ,[ randomString(55) for i in range(chunksize)]    #Pharmacy Location Street Address
        ,[ randomString(2) for i in range(chunksize)]    #Pharmacy Location State
        ,[ randomString(9) for i in range(chunksize)]    #Pharmacy ZIP Code 
        ,[ randomString(2) for i in range(chunksize)]    #Pharmacy Country Code
        ,[ randomString(1) for i in range(chunksize)]    #Mail-Order Pharmacy Indicator
        ,[ randomString(8) for i in range(chunksize)]    #Carrier Associated with Claim
        ,[ randomString(1) for i in range(chunksize)]    #In Plan Network Indicator
        ,[ randomString(1) for i in range(chunksize)]    #Record Status Code 
        ,[ randomString(1) for i in range(chunksize)]    #Claim Line Type 
        ,[ randomString(3) for i in range(chunksize)]    #Reject Code 
        ,[ randomString(50) for i in range(chunksize)]    #Carrier Specific Unique Member ID
        ,[ randomString(50) for i in range(chunksize)]    #Carrier Specific Unique Subscriber ID
        ,[ randomString(10) for i in range(chunksize)]    #Prescriber Specialty 
        ,[ randomString(30) for i in range(chunksize)]    #Pharmacy City 
        ,[ randomString(1) for i in range(chunksize)]    #Unassigned 
        ,[ 'PC' for i in range(chunksize)]    #Record Type 
                   
    ]        
            
            
    rows = ['%s|%s|%s|%s|%s|%i|%i|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|'\
            '%s|%s|%s|%s|%s|%s|%i|%i|%s|%s|%i|%i|%i|%i|%i|%i|%i|%i|%i|%i|%i|%i|%i|%s|%s|'\
            '%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n' % row for row in zip(*data)]
    
    
    tdelta1 = time.time() - t1
    print('rx - data gen', tdelta1)
    
    
    
    
    t0 = time.time()

    with open(outfile, 'a') as pharmFile:
        pharmFile.writelines(rows)
        
    tdelta0 = time.time() - t0
    print('rx - data load', tdelta0)            
    
    loop_counter = loop_counter - 1    
    
    data = []
    gc.collect()
        
  
    
with open(outfile, 'a') as pharmFile:    
  pharmFile.writelines(trailer_line)
  pharmFile.close()