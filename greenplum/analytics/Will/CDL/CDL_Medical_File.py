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
outfile = export_path + 'Medical_07112022.txt'

chunksize = 100_000

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



header_line = ['HD|1111111|PAYER11|SAMPLE FILE|MC|201907|202010|T|This is a sample MEDICAL file\n']

trailer_line = ['TR|1111111|PAYER11|SAMPLE FILE|MC|20211007||2376']

column_names = ['CDLMC001|CDLMC002|CDLMC003|CDLMC004|CDLMC005|CDLMC006|CDLMC007|CDLMC008|'\
                'CDLMC009|CDLMC010|CDLMC011|CDLMC012|CDLMC013|CDLMC014|CDLMC015|CDLMC016|'\
                'CDLMC017|CDLMC018|CDLMC019|CDLMC020|CDLMC021|CDLMC022|CDLMC023|CDLMC024|'\
                'CDLMC025|CDLMC026|CDLMC027|CDLMC028|CDLMC029|CDLMC030|CDLMC031|CDLMC032|'\
                'CDLMC033|CDLMC034|CDLMC035|CDLMC036|CDLMC037|CDLMC038|CDLMC039|CDLMC040|'\
                'CDLMC041|CDLMC042|CDLMC043|CDLMC044|CDLMC045|CDLMC046|CDLMC047|CDLMC048|'\
                'CDLMC049|CDLMC050|CDLMC051|CDLMC052|CDLMC053|CDLMC054|CDLMC055|CDLMC056|'\
                'CDLMC057|CDLMC058|CDLMC059|CDLMC060|CDLMC061|CDLMC062|CDLMC063|CDLMC064|'\
                'CDLMC065|CDLMC066|CDLMC067|CDLMC068|CDLMC069|CDLMC070|CDLMC071|CDLMC072|'\
                'CDLMC073|CDLMC074|CDLMC075|CDLMC076|CDLMC077|CDLMC078|CDLMC079|CDLMC080|'\
                'CDLMC081|CDLMC082|CDLMC083|CDLMC084|CDLMC085|CDLMC086|CDLMC087|CDLMC088|'\
                'CDLMC089|CDLMC090|CDLMC091|CDLMC092|CDLMC093|CDLMC094|CDLMC095|CDLMC096|'\
                'CDLMC097|CDLMC098|CDLMC099|CDLMC100|CDLMC101|CDLMC102|CDLMC103|CDLMC104|'\
                'CDLMC105|CDLMC106|CDLMC107|CDLMC108|CDLMC109|CDLMC110|CDLMC111|CDLMC112|'\
                'CDLMC113|CDLMC114|CDLMC115|CDLMC116|CDLMC117|CDLMC118|CDLMC119|CDLMC120|'\
                'CDLMC121|CDLMC122|CDLMC123|CDLMC124|CDLMC125|CDLMC126|CDLMC127|CDLMC128|'\
                'CDLMC129|CDLMC130|CDLMC131|CDLMC132|CDLMC133|CDLMC134|CDLMC135|CDLMC136|'\
                'CDLMC137|CDLMC138|CDLMC139|CDLMC140|CDLMC141|CDLMC142|CDLMC143|CDLMC144|'\
                'CDLMC145|CDLMC146|CDLMC147|CDLMC148|CDLMC149|CDLMC150|CDLMC151|CDLMC152|'\
                'CDLMC153|CDLMC154|CDLMC155|CDLMC156|CDLMC157|CDLMC158|CDLMC159|CDLMC160|'\
                'CDLMC161|CDLMC162|CDLMC163|CDLMCXXX|CDLMC899\n']

with open(outfile, 'a') as medFile:
    medFile.writelines(header_line) 
    medFile.writelines(column_names)    

loop_counter = 25 

while loop_counter > 0:
    
    t1 = time.time()       
    
    data = [ 
             [ '1111111' for i in range(chunksize)]    #Data Submitter Code
            ,[ 'PAYER11' for i in range(chunksize)]    #Payor Code 
            ,[ randomString(30) for i in range(chunksize)]    #Plan ID 
            ,[ randomString(2) for i in range(chunksize)]    #Member Insurance/ProductCategory Code
            ,[ randomString(35) for i in range(chunksize)]    #Payor Claim ControlNumber
            ,[ randomInt(4) for i in range(chunksize)]    #Line Counter 
            ,[ randomInt(4) for i in range(chunksize)]    #Version Number 
            ,[ randomString(35) for i in range(chunksize)]    #Cross Reference Claims ID
            ,[ randomString(50) for i in range(chunksize)]    #Insured Group or Policy Number
            ,[ randomString(4) for i in range(chunksize)]    #Medicaid AID Category 
            ,[ randomString(9) for i in range(chunksize)]    #Subscriber Social Security Number
            ,[ randomString(80) for i in range(chunksize)]    #Plan Specific ContractNumber
            ,[ randomString(60) for i in range(chunksize)]    #Subscriber Last Name 
            ,[ randomString(35) for i in range(chunksize)]    #Subscriber First Name 
            ,[ randomString(20) for i in range(chunksize)]    #Sequence Number 
            ,[ randomString(9) for i in range(chunksize)]    #Member Social SecurityNumber
            ,[ randomString(2) for i in range(chunksize)]    #Individual RelationshipCode
            ,[ randomString(1) for i in range(chunksize)]    #Member Gender 
            ,[ randomString(8) for i in range(chunksize)]    #Member Date of Birth
            ,[ randomString(60) for i in range(chunksize)]    #Member Last Name 
            ,[ randomString(35) for i in range(chunksize)]    #Member First Name 
            ,[ randomString(9) for i in range(chunksize)]    #Member ZIP Code 
            ,[ randomString(20) for i in range(chunksize)]    #Patient Control Number
            ,[ randomString(8) for i in range(chunksize)]    #Paid Date 
            ,[ randomString(8) for i in range(chunksize)]    #Admission Date 
            ,[ randomString(4) for i in range(chunksize)]    #Admission Hour 
            ,[ randomString(1) for i in range(chunksize)]    #Admission Type 
            ,[ randomString(1) for i in range(chunksize)]    #Point of Origin 
            ,[ randomString(8) for i in range(chunksize)]    #Discharge Date 
            ,[ randomString(4) for i in range(chunksize)]    #Discharge Hour 
            ,[ randomString(2) for i in range(chunksize)]    #Discharge Status 
            ,[ randomString(3) for i in range(chunksize)]    #Type of Bill –Institutional 
            ,[ randomString(2) for i in range(chunksize)]    #Place of Service –Professional
            ,[ randomString(7) for i in range(chunksize)]    #Admitting Diagnosis 
            ,[ randomString(7) for i in range(chunksize)]    #First External Cause Code 
            ,[ randomString(1) for i in range(chunksize)]    #ICD Version Indicator
            ,[ randomString(7) for i in range(chunksize)]    #Principal Diagnosis 
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 1 
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 2 
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 3 
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 4 
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 5 
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 6 
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 7 
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 8 
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 9 
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 10
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 11
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 12
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 13
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 14
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 15
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 16
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 17
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 18
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 19
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 20
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 21
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 22
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 23
            ,[ randomString(7) for i in range(chunksize)]    #Other Diagnosis – 24
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode – 01
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode – 02
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode – 03
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode – 04
            ,[ randomString(1) for i in range(chunksize)]    #Present onAdmissionCode -05
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode – 06
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode – 07
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode – 08
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode – 09
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode – 10
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode – 11
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode – 12
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode – 13
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode – 14
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode – 15
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode – 16
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode – 17
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode – 18
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode – 19
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode – 20
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode – 21
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode– 22
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode – 23
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode – 24
            ,[ randomString(1) for i in range(chunksize)]    #Present on AdmissionCode – 25
            ,[ randomString(4) for i in range(chunksize)]    #Revenue Code 
            ,[ randomString(5) for i in range(chunksize)]    #Procedure Code 
            ,[ randomString(2) for i in range(chunksize)]    #Procedure Modifier – 1 
            ,[ randomString(2) for i in range(chunksize)]    #Procedure Modifier – 2 
            ,[ randomString(2) for i in range(chunksize)]    #Procedure Modifier – 3 
            ,[ randomString(2) for i in range(chunksize)]    #Procedure Modifier – 4 
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-PCS  Principal Procedure Code
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 1
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 2
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 3
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 4
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 5
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 6
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 7
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 8
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 9
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 10
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 11
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 12
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 13
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 14
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 15
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 16
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 17
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 18
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 19
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 20
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 21
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 22
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 23
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 24
            ,[ randomString(7) for i in range(chunksize)]    #ICD-9 CM/10-CMPCS Other Procedure Code – 25
            ,[ randomString(8) for i in range(chunksize)]    #Date of Service – From
            ,[ randomString(8) for i in range(chunksize)]    #Date of Service – Through
            ,[ randomString(12) for i in range(chunksize)]    #Service Units/Quantity
            ,[ randomString(2) for i in range(chunksize)]    #Unit of Measure 
            ,[ randomInt(12) for i in range(chunksize)]    #Charge Amount 
            ,[ randomInt(12) for i in range(chunksize)]    #Withhold Amount 
            ,[ randomInt(12) for i in range(chunksize)]    #Plan Paid Amount 
            ,[ randomInt(12) for i in range(chunksize)]    #Copay Amount 
            ,[ randomInt(12) for i in range(chunksize)]    #Coinsurance Amount 
            ,[ randomInt(12) for i in range(chunksize)]    #Deductible Amount 
            ,[ randomInt(12) for i in range(chunksize)]    #Other Insurance Paid Amount
            ,[ randomInt(12) for i in range(chunksize)]    #COB/TPL Amount 
            ,[ randomInt(12) for i in range(chunksize)]    #Allowed Amount 
            ,[ randomString(2) for i in range(chunksize)]    #Payment Arrangement Type Indicator
            ,[ randomString(11) for i in range(chunksize)]    #Drug Code 
            ,[ randomString(35) for i in range(chunksize)]    #Rendering Provider ID 
            ,[ randomString(10) for i in range(chunksize)]    #Rendering Provider NPI 
            ,[ randomString(1) for i in range(chunksize)]    #Rendering Provider Entity Type Qualifier
            ,[ randomString(1) for i in range(chunksize)]    #In Plan Network Indicator
            ,[ randomString(35) for i in range(chunksize)]    #Rendering Provider First Name
            ,[ randomString(25) for i in range(chunksize)]    #Rendering Provider Middle Name
            ,[ randomString(60) for i in range(chunksize)]    #Rendering Provider Last Name or Organization Name
            ,[ randomString(10) for i in range(chunksize)]    #Rendering Provider Suffix
            ,[ randomString(10) for i in range(chunksize)]    #Rendering Provider Specialty
            ,[ randomString(30) for i in range(chunksize)]    #Rendering Provider City Name
            ,[ randomString(2) for i in range(chunksize)]    #Rendering Provider State or Province
            ,[ randomString(9) for i in range(chunksize)]    #Rendering Provider ZIP Code
            ,[ randomString(10) for i in range(chunksize)]    #Rendering Provider Group Practice NPI
            ,[ randomString(30) for i in range(chunksize)]    #Billing Provider ID 
            ,[ randomString(10) for i in range(chunksize)]    #Billing Provider NPI 
            ,[ randomString(60) for i in range(chunksize)]    #Billing Provider Last Name or Organization Name
            ,[ randomString(10) for i in range(chunksize)]    #Billing Provider Tax ID
            ,[ randomString(30) for i in range(chunksize)]    #Referring Provider ID 
            ,[ randomString(10) for i in range(chunksize)]    #Referring Provider NPI 
            ,[ randomString(30) for i in range(chunksize)]    #Attending Provider ID 
            ,[ randomString(10) for i in range(chunksize)]    #Attending Provider NPI 
            ,[ randomString(8) for i in range(chunksize)]    #Carrier Associated with Claim
            ,[ randomString(1) for i in range(chunksize)]    #Type of Claim 
            ,[ randomString(2) for i in range(chunksize)]    #Claim Status 
            ,[ randomString(1) for i in range(chunksize)]    #Denied Claim Line Indicator
            ,[ randomString(3) for i in range(chunksize)]    #Claim Adjustment Reason Code
            ,[ randomString(1) for i in range(chunksize)]    #Claim Line Type 
            ,[ randomString(50) for i in range(chunksize)]    #Carrier Specific Unique Member ID
            ,[ randomString(50) for i in range(chunksize)]    #Carrier Specific UniqueSubscriber ID
            ,[ randomString(55) for i in range(chunksize)]    #Rendering Provider Street Address
            ,[ randomString(1) for i in range(chunksize)]    #Unassigned 
            ,[ 'MC' for i in range(chunksize)]    #Record Type 
    ]        
            
            
    rows = ['%s|%s|%s|%s|%s|%i|%i|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|'\
            '%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|'\
            '%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|'\
            '%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|'\
            '%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|'\
            '%s|%s|%s|%s|%s|%s|%s|%i|%i|%i|%i|%i|%i|%i|%i|%i|%s|%s|%s|%s|%s|%s|%s|'\
            '%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|'\
            '%s|%s|%s|%s\n' % row for row in zip(*data)]
    
    
    tdelta1 = time.time() - t1
    print('Med - data gen', tdelta1)
    
    
    
    
    t0 = time.time()

    with open(outfile, 'a') as medFile:
        medFile.writelines(rows)
        
    tdelta0 = time.time() - t0
    print('Med - data load', tdelta0)            
    
    loop_counter = loop_counter - 1    
    
    data = []
    gc.collect()
        
  
    
with open(outfile, 'a') as medFile:    
  medFile.writelines(trailer_line)
  medFile.close()