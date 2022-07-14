# -*- coding: utf-8 -*-
"""
Created on Sun Jul 10 13:42:10 2022

@author: wcoughlin
"""


import random
import string 
from faker import Faker
import time
import os 


work_path = 'S:\\APCD\\TestData\\Work\\'

export_path = 'S:\\APCD\\TestData\\'
outfile = export_path + 'Eligibility_small_test.txt'

chunksize = 999_000

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



header_line = ['HD|1111111|PAYER11|SAMPLE FILE|ME|201907|202010|T|This is a sample ELIGIBILITY file\n']

trailer_line = ['TR|1111111|PAYER11|SAMPLE FILE|ME|20211007||2376']

column_names = ['CDLMC001|CDLMC002|CDLMC003|CDLMC004|CDLMC005|CDLMC006|CDLMC007|CDLMC008'\
                'CDLMC009|CDLMC010|CDLMC011|CDLMC012|CDLMC013|CDLMC014|CDLMC015|CDLMC016'\
                'CDLMC017|CDLMC018|CDLMC019|CDLMC020|CDLMC021|CDLMC022|CDLMC023|CDLMC024'\
                'CDLMC025|CDLMC026|CDLMC027|CDLMC028|CDLMC029|CDLMC030|CDLMC031|CDLMC032'\
                'CDLMC033|CDLMC034|CDLMC035|CDLMC036|CDLMC037|CDLMC038|CDLMC039|CDLMC040'\
                'CDLMC041|CDLMC042|CDLMC043|CDLMC044|CDLMC045|CDLMC046|CDLMC047|CDLMC048'\
                'CDLMC049|CDLMC050|CDLMC051|CDLMC052|CDLMC053|CDLMC054|CDLMC055|CDLMC056'\
                'CDLMC057|CDLMC058|CDLMC059|CDLMC060|CDLMC061|CDLMC062|CDLMC063|CDLMC064'\
                'CDLMC065|CDLMC066|CDLMC067|CDLMC068|CDLMC069|CDLMC070|CDLMC071|CDLMC072'\
                'CDLMC073|CDLMC074|CDLMC075|CDLMC076|CDLMC077|CDLMC078|CDLMC079|CDLMC080'\
                'CDLMC081|CDLMC082|CDLMC083|CDLMC084|CDLMC085|CDLMC086|CDLMC087|CDLMC088'\
                'CDLMC089|CDLMC090|CDLMC091|CDLMC092|CDLMC093|CDLMC094|CDLMC095|CDLMC096'\
                'CDLMC097|CDLMC098|CDLMC099|CDLMC100|CDLMC101|CDLMC102|CDLMC103|CDLMC104'\
                'CDLMC105|CDLMC106|CDLMC107|CDLMC108|CDLMC109|CDLMC110|CDLMC111|CDLMC112'\
                'CDLMC113|CDLMC114|CDLMC115|CDLMC116|CDLMC117|CDLMC118|CDLMC119|CDLMC120'\
                'CDLMC121|CDLMC122|CDLMC123|CDLMC124|CDLMC125|CDLMC126|CDLMC127|CDLMC128'\
                'CDLMC129|CDLMC130|CDLMC131|CDLMC132|CDLMC133|CDLMC134|CDLMC135|CDLMC136'\
                'CDLMC137|CDLMC138|CDLMC139|CDLMC140|CDLMC141|CDLMC142|CDLMC143|CDLMC144'\
                'CDLMC145|CDLMC146|CDLMC147|CDLMC148|CDLMC149|CDLMC150|CDLMC151|CDLMC152'\
                'CDLMC153|CDLMC154|CDLMC155|CDLMC156|CDLMC157|CDLMC158|CDLMC159|CDLMC160'\
                'CDLMC161|CDLMC162|CDLMC163|CDLMCXXX|CDLMC899']

    
t1 = time.time()    
    
data = [
          [ randomString(8) for i in range(chunksize)] #Data Submitter Code--  varchar 8
        , [ randomString(8) for i in range(chunksize)] #Payor Code --  varchar 8
        , [ randomString(30) for i in range(chunksize)] #Plan ID --  varchar 30
        , [ randomInt(2) for i in range(chunksize)] #MemberInsurance/ProductCategory Code--  char 2
        , [ random.randint(2020,2021) for i in range(chunksize)] #Start Year ofSubmission --  int 4
        , [ random.randint(1,12) for i in range(chunksize)] #Start Monthof Submission --  int 2
        , [ randomString(50) for i in range(chunksize)] #Insured Group or Policy Number--  varchar 50
        , [ randomInt(3) for i in range(chunksize)] #Coverage Level Code --  char 3
        , [ randomInt(4) for i in range(chunksize)] #Medicaid AID Category --  char 4
        , [ fake.ssn().replace('-','')  for i in range(chunksize)] #Subscriber Social Security Number--  char 9
        , [ randomString(80) for i in range(chunksize)] #Plan Specific Contract Number--  varchar 80
        , [ randomString(20)  for i in range(chunksize)] #Subscriber Last Name --  varchar 60
        , [ randomString(20)  for i in range(chunksize)] #Subscriber First Name --  varchar 35
        , [ randomString(1) for i in range(chunksize)] #Subscriber Middle Initial --  char 1
        , [ randomString(20) for i in range(chunksize)] #Sequence Number --  varchar 20
        , [ randomInt(9) for i in range(chunksize)] #Member Social SecurityNumber--  char 9
        , [ randomInt(2) for i in range(chunksize)] #Individual RelationshipCode--  char 2
        , [ random.choice(gender_list) for i in range(chunksize)] #Member Gender --  char 1
        , [ date_generator ('-100y','today') for i in range(chunksize)] #Member Date of Birth --  date 8
        , [ randomString(20)  for i in range(chunksize)] #Member Last Name --  varchar 60
        , [ randomString(20)  for i in range(chunksize)] #Member First Name --  varchar 35
        , [ randomString(1) for i in range(chunksize)] #Member Middle Initial --  char 1
        , [ fake.street_address() for i in range(chunksize)] #Member Street Address--  varchar 55
        , [ fake.city() for i in range(chunksize)] #Member City Name --  varchar 30
        , [ randomInt(2) for i in range(chunksize)] #Member State or Province--  char 2
        , [ fake['en-US'].postcode().rjust(5,'0') for i in range(chunksize)] #Member ZIP Code --  varchar 9
        , [ randomInt(5) for i in range(chunksize)] #Member FIPs County Code --  char 5
        , [ randomInt(2) for i in range(chunksize)] #Member Country Code --  char 2
        , [ randomString(2) for i in range(chunksize)] #Race 1 --  varchar 2
        , [ randomString(2) for i in range(chunksize)] #Race 2 --  varchar 2
        , [ randomString(2) for i in range(chunksize)] #Race 3 --  varchar 2
        , [ randomInt(1) for i in range(chunksize)] #Hispanic Indicator --  char 1
        , [ randomString(6) for i in range(chunksize)] #Ethnicity 1 --  varchar 6
        , [ randomString(6) for i in range(chunksize)] #Ethnicity 2 --  varchar 6
        , [ randomString(6) for i in range(chunksize)] #Other Ethnicity --  varchar 6
        , [ randomInt(1) for i in range(chunksize)] #Medical Coverage    Under This Plan--  char 1
        , [ randomInt(1) for i in range(chunksize)] #Pharmacy Coverage    Under This Plan--  char 1
        , [ randomInt(1) for i in range(chunksize)] #Dental Coverage    Under This Plan--  char 1
        , [ randomInt(1) for i in range(chunksize)] #Behavioral Health    Coverage Under This    Plan--  char 1
        , [ randomInt(1) for i in range(chunksize)] #Primary Insurance Indicator--  char 1
        , [ randomInt(3) for i in range(chunksize)] #Coverage Type --  char 3
        , [ randomInt(2) for i in range(chunksize)] #Plan State --  char 2
        , [ randomString(4) for i in range(chunksize)] #Market Category Code--  varchar 4
        , [ randomString(6) for i in range(chunksize)] #Special Coverage --  varchar 6
        , [ randomString(60) for i in range(chunksize)] #Group Name --  varchar 60
        , [ randomString(35) for i in range(chunksize)] #Member PCP ID --  varchar 35
        , [ randomInt(10) for i in range(chunksize)] #NPI of Member’s PCP--  char 10
        , [ randomInt(1) for i in range(chunksize)] #PCP Assignment --  char 1
        , [ date_generator ( '-1y', 'today') for i in range(chunksize)] #Member PCP Effective Date --  date 8
        , [ date_generator ( '-1y', 'today') for i in range(chunksize)] #Plan Effective Date --  date 8
        , [ date_generator ( '-1y', 'today') for i in range(chunksize)] #Plan Term Date --  date 8
        , [ randomString(1) for i in range(chunksize)] #HIOS Plan Indicator --  varchar 1
        , [ randomString(16) for i in range(chunksize)] #HIOS Plan ID --  varchar 16
        , [ randomInt(1) for i in range(chunksize)] #Metal Tier --  char 1
        , [ randomInt(1) for i in range(chunksize)] #Medical Home Indicator--  char 1
        , [ randomString(30) for i in range(chunksize)] #Payor Assigned ID    for Medical Home--  varchar 30
        , [ randomInt(1) for i in range(chunksize)] #Enrolled Through a Public Health Insurance    Exchange--  char 1
        , [ randomString(10) for i in range(chunksize)] #Employer Tax ID --  varchar 10
        , [ randomInt(1) for i in range(chunksize)] #Employment Status --  char 1
        , [ randomString(9) for i in range(chunksize)] #Employer ZIP Code --  varchar 9
        , [ randomString(50) for i in range(chunksize)] #Carrier Specific Unique    Member ID--  varchar 50
        , [ randomString(50) for i in range(chunksize)] #Carrier Specific Unique    Subscriber ID--  varchar 50
        , [ randomInt(5) for i in range(chunksize)] #NAIC ID --  char 5
        , [ randomInt(1) for i in range(chunksize)] #High Deductible Plan Indicator--  char 1
        , [ randomInt(12) for i in range(chunksize)] #Total Monthly Premium    Amount--  int 12
        , [ randomInt(6) for i in range(chunksize)] #Actuarial Value -- dec 6, 4
        , [ randomInt(1) for i in range(chunksize)] #Grandfathered Plan    Indicator--  char 1
        , [ randomInt(1) for i in range(chunksize)] #Cost-Sharing Reduction    Indicator--  char 1
        , [ randomInt(12) for i in range(chunksize)] #Administrative Service Fees--  int 12
        , [ randomInt(1) for i in range(chunksize)] #Tiered Network --  char 1
        , [ randomInt(1) for i in range(chunksize)] #Member Income Frequency Code--  char 1
        , [ randomInt(12) for i in range(chunksize)] #Member Income Monetary Amount--  int 12
        , [ randomInt(3) for i in range(chunksize)] #Member Primary Language--  char 3
        , [ randomString(11) for i in range(chunksize)] #Subscriber Medicare    Beneficiary Identifier--  varchar 11
        , [ randomString(11) for i in range(chunksize)] #Member Medicare    Beneficiary Identifier--  varchar 11
        , [ randomString(30) for i in range(chunksize)] #ACO Identifier --  varchar 30
        , [ randomString(60) for i in range(chunksize)] #ACO Name --  varchar 60
        , [ randomString(30) for i in range(chunksize)] #Physician Organization    Identifier--  varchar 30
        , [ randomInt(1) for i in range(chunksize)] #Un-assigned --  char 1
        , [ randomInt(1) for i in range(chunksize)] #Original Reason for Entitlement (OREC)--  char1
        , [ randomInt(1) for i in range(chunksize)] #Current Reason for Entitlement Code (CREC)--  char1
        , [ randomInt(1) for i in range(chunksize)] #End state renal disease Indicator     (ESRD_IND)--  char1
        , [ randomInt(1) for i in range(chunksize)] #Medicare Status Code (MS-CD)--  char1
        , [ randomInt(8) for i in range(chunksize)] #Death Date     (DEATH_DT)--  date8
        , [ randomInt(12) for i in range(chunksize)] #Pure Rate    (PURE_RATE)--  int 12
        , [ randomInt(2) for i in range(chunksize)] #Managed Care Organization ID    (MCO_ID)--  char2
        , [ randomInt(2) for i in range(chunksize)] #Plan Code    (PLAN_CD)--  char2
        , [ randomInt(2) for i in range(chunksize)] #Family Size    (FAM_SIZE)--  int 2
        , [ randomInt(1) for i in range(chunksize)] #Education--  char1
        , [ randomInt(6) for i in range(chunksize)] #Case Number    (CASE_NBR)--  int 6
        , [ randomInt(8) for i in range(chunksize)] #Supplementary Medical Insurance Benefits Start Date    (SMIB_FROM_DT)--  date8
        , [ randomInt(8) for i in range(chunksize)] #Supplementary Medical Insurance Benefits End Date    (SMIB_TO_DT)--  date8
        , [ randomInt(1) for i in range(chunksize)] #TX_HOLD--  char1
        , [ randomInt(1) for i in range(chunksize)] #Managed Care Indicator    (MC_FLAG)--  char1
        , [ randomInt(2) for i in range(chunksize)] #Managed Care Stopped Coverage Reason Code    (MC_SC)--  char2
        , [ randomInt(1) for i in range(chunksize)] #Category Code    (ME_CAT)--  char1
        , [ randomInt(1) for i in range(chunksize)] #Medicaid Eligibility Code    (ME_CODE)--  char1
        , [ randomInt(2) for i in range(chunksize)] #Medicaid Type Program Code    (ME_TP)--  char2
        , [ randomInt(1) for i in range(chunksize)] #Member Spend Down    ME_SD--  char1
        , [ randomInt(3) for i in range(chunksize)] #Risk Group Identifier    (RISKGRP_ID)--  char3
        , [ randomInt(12) for i in range(chunksize)] #Family Income    (FAM_INCOME)--  int 12
        , [ randomInt(1) for i in range(chunksize)] #Status In Group    (SIG)--  char1
        , [ randomInt(1) for i in range(chunksize)] #Supplementary Medical Insurance Benefits    (SMIB)--  char1
        , [ randomInt(2) for i in range(chunksize)] #Base Plan    (BASE_PLAN)--  char2
        , [ randomInt(6) for i in range(chunksize)] #Eligibility Date    (ELIG_DATE)--  date6
        , [ randomInt(5) for i in range(chunksize)] #Texas County Identifier--  int 5
        , [ randomInt(1) for i in range(chunksize)] #Dental Plan Indicator--  char1
        , [ randomInt(2) for i in range(chunksize)] #Record Type --  char 2

]
rows = ['%s|%s|%s|%i|%i|%i|%s|%i|%i|%s|%s|%s|%s|%s|%s|%i|%i|%s|%s|%s|%s|%s|%s|%s|%i|'\
        '%s|%i|%i|%s|%s|%s|%i|%s|%s|%s|%i|%i|%i|%i|%i|%i|%i|%s|%s|%s|%s|%i|%i|%s|%s|'\
        '%s|%s|%s|%i|%i|%s|%i|%s|%i|%s|%s|%s|%i|%i|%i|%i|%i|%i|%i|%i|%i|%i|%i|%s|%s|'\
        '%s|%s|%s|%i|%i|%i|%i|%i|%i|%i|%i|%i|%i|%i|%i|%i|%i|%i|%i|%i|%i|%i|%i|%i|%i|'\
        '%i|%i|%i|%i|%i|%i|%i|%i|\n' % row for row in zip(*data)]


tdelta1 = time.time() - t1
print('data gen', tdelta1)

    
elig_header = work_path + 'EligHeader.txt'
elig_trailer = work_path + 'EligTrailer.txt'

filenames = [elig_header]    


t0 = time.time()

with open(outfile, 'a') as eligFile:
    eligFile.writelines(header_line) 
    eligFile.writelines(column_names)
    eligFile.writelines(rows)
    eligFile.writelines(trailer_line)
    
tdelta0 = time.time() - t0
print('data load', tdelta0)








