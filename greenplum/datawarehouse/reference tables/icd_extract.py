#must have the zip files from cms as seen in cms_icd_zip_getter.py
#note in 2020 and 2021 ensure all files are directly below the directory
#in order to be read

import os
import csv
import pandas as pd

output_dir = './zip_output'

icd_code_files = [
 'ICD10_2021\\icd10cm_order_2021.txt',
 'ICD10_2020\\icd10cm_order_2020.txt',
 'ICD10_2019\\icd10cm_order_2019.txt',
 'ICD10_2018\\icd10cm_order_2018.txt',
 'ICD10_2017\\icd10cm_order_2017.txt',
 'ICD10_2016\\icd10cm_order_2016.txt',
 'ICD10_2015\\icd10cm_order_2015.txt',
 'ICD10_2014\\icd10cm_order_2014.txt',
 'ICD9_2014\\CMS32_DESC_SHORT_DX.txt',
 'ICD9_2013\\CMS31_DESC_SHORT_DX.txt',
 'ICD9_2012\\CMS30_DESC_SHORT_DX.txt',
 'ICD9_2011\\CMS29_DESC_SHORT_DX.txt',
 'ICD9_2010\\CMS28_DESC_SHORT_DX.txt',
 'ICD9_2009\\V27LONG_SHORT_DX_110909.csv',
 'ICD9_2008\\V26 I-9 Diagnosis.txt',
 'ICD9_2007\\I9diagnosesV25.txt',
 'ICD9_2006\\I9diagnosis.txt',
 'ICD9_2005\\I9DX_DESC.txt'
 ]

# {'icd_version': '', 'icd_code':'', 'icd_year':'', 'code_description':''}
total_icd_values = []

#check to ensure files have the same format
for file in icd_code_files:
    file_path = os.path.join(output_dir, file)
    print(file)
    icd_year = int(file.split('_')[1][:4])

    with open(file_path) as f:
        if file.endswith('.txt'):
            for line in f:
                line = line.strip()
                if file.startswith('ICD10'):
                    total_icd_values.append({'icd_type': '10', 'diag_cd':line[6:13].strip(), 'icd_year':icd_year, 'code_description':line[16:76].strip()})
                else:
                    total_icd_values.append({'icd_type': '9', 'diag_cd':line[0:4].strip(), 'icd_year':icd_year, 'code_description':line[5:].strip()})
        else:
            csv_reader = csv.reader(f)
            header = next(csv_reader)
            for line in csv_reader:
                total_icd_values.append({'icd_type':'9', 'diag_cd':line[0].strip(), 'icd_year':icd_year, 'code_description':line[2]})

total_icd_values = pd.DataFrame(total_icd_values)

total_icd_values = total_icd_values.groupby(['icd_type', 'diag_cd']).agg(initial_year=('icd_year','min'),
                                                                          last_year=('icd_year','max'), 
                                                                          code_description=('code_description', 'first'))



total_icd_values.to_csv('total_icd_codes.csv')