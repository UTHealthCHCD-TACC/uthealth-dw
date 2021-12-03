import requests
import zipfile
import io
import os

zip_icd_file_urls = {
'ICD10_2021':'https://www.cms.gov/files/zip/2021-code-descriptions-tabular-order-updated-12162020.zip',
'ICD10_2020':'https://www.cms.gov/Medicare/Coding/ICD10/Downloads/2020-ICD-10-CM-Codes.zip',
'ICD10_2019':'https://www.cms.gov/Medicare/Coding/ICD10/Downloads/2019-ICD-10-CM-Code-Descriptions.zip',
'ICD10_2018':'https://www.cms.gov/Medicare/Coding/ICD10/Downloads/2018-ICD-10-Code-Descriptions.zip',
'ICD10_2017':'https://www.cms.gov/Medicare/Coding/ICD10/Downloads/2017-ICD10-Code-Descriptions.zip',
'ICD10_2016':'https://www.cms.gov/Medicare/Coding/ICD10/Downloads/2016-Code-Descriptions-in-Tabular-Order.zip',
'ICD10_2015':'https://www.cms.gov/Medicare/Coding/ICD10/Downloads/2015-code-descriptions.zip',
'ICD10_2014':'https://www.cms.gov/Medicare/Coding/ICD10/Downloads/2014-ICD10-Code-Descriptions.zip',
'ICD9_2014':'https://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/Downloads/ICD-9-CM-v32-master-descriptions.zip',
'ICD9_2013':'https://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/Downloads/cmsv31-master-descriptions.zip',
'ICD9_2012':'https://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/Downloads/cmsv30_master_descriptions.zip',
'ICD9_2011':'https://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/Downloads/cmsv29_master_descriptions.zip',
'ICD9_2010':'https://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/Downloads/cmsv28_master_descriptions.zip',
'ICD9_2009':'https://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/Downloads/FY2010Diagnosis-ProcedureCodesFullTitles.zip',
'ICD9_2008':'https://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/Downloads/v26_icd9.zip',
'ICD9_2007':'https://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/Downloads/v25_icd9.zip',
'ICD9_2006':'https://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/Downloads/v24_icd9.zip',
'ICD9_2005':'https://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/Downloads/v23_icd9.zip'
}


output_dir = './zip_output'

os.mkdir(output_dir)

for year, url in zip_icd_file_urls.items():
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(os.path.join(output_dir, year))

