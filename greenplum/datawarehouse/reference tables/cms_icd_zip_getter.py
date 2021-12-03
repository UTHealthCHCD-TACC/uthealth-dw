import requests
import zipfile
import io
import os

zip_icd_cm_file_urls = {
'ICD10_2022':'https://www.cms.gov/files/zip/2022-code-descriptions-tabular-order.zip',
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

#icd 9-pcs is with icd 9 cm codes
zip_icd_pcs_file_urls = {
'ICD10_2022': 'https://www.cms.gov/files/zip/2022-icd-10-pcs-order-file-long-and-abbreviated-titles-updated-december-1-2021.zip',
'ICD10_2021': 'https://www.cms.gov/files/zip/2021-icd-10-pcs-order-file-long-and-abbreviated-titles-updated-december-1-2020.zip',
'ICD10_2020': 'https://www.cms.gov/Medicare/Coding/ICD10/Downloads/2019-ICD-10-PCS-Order-File.zip',
'ICD10_2019': 'https://www.cms.gov/Medicare/Coding/ICD10/Downloads/2019-ICD-10-PCS-Order-File.zip',
'ICD10_2018': 'https://www.cms.gov/Medicare/Coding/ICD10/Downloads/2018-ICD-10-PCS-Order-File.zip',
'ICD10_2017': 'https://www.cms.gov/Medicare/Coding/ICD10/Downloads/2017-PCS-Long-Abbrev-Titles.zip',
'ICD10_2016': 'https://www.cms.gov/Medicare/Coding/ICD10/Downloads/2016-PCS-Long-Abbrev-Titles.zip',
'ICD10_2015': 'https://www.cms.gov/Medicare/Coding/ICD10/Downloads/2015-PCS-long-and-abbreviated-titles.zip',
'ICD10_2014': 'https://www.cms.gov/Medicare/Coding/ICD10/Downloads/2014-PCS-long-and-abbreviated-titles.zip',
'ICD9_2014': 'https://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/Downloads/ICD-9-CM-v32-master-descriptions.zip',
'ICD9_2013': 'https://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/Downloads/cmsv31-master-descriptions.zip',
'ICD9_2012': 'https://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/Downloads/cmsv30_master_descriptions.zip',
'ICD9_2011': 'https://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/Downloads/cmsv29_master_descriptions.zip',
'ICD9_2010': 'https://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/Downloads/cmsv28_master_descriptions.zip',
'ICD9_2009': 'https://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/Downloads/FY2010Diagnosis-ProcedureCodesFullTitles.zip',
'ICD9_2008': 'https://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/Downloads/v26_icd9.zip',
'ICD9_2007': 'https://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/Downloads/v25_icd9.zip',
'ICD9_2006': 'https://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/Downloads/v24_icd9.zip',
'ICD9_2005': 'https://www.cms.gov/Medicare/Coding/ICD9ProviderDiagnosticCodes/Downloads/v23_icd9.zip'
}

#icd 9-pcs is with icd 9 cm codes; does not allow for programmatic access via requests
zip_cpt_file_urls = {
'CPT_HCPCS_2022': 'https://www.cms.gov/apps/ama/license.asp?file=/files/zip/list-codes-effective-january-1-2022-published-november-19-2021.zip',
'CPT_HCPCS_2021': 'https://www.cms.gov/apps/ama/license.asp?file=/files/zip/list-codes-effective-january-1-2021-issued-december-1-2020.zip',
'CPT_HCPCS_2020': 'https://www.cms.gov/apps/ama/license.asp?file=/files/zip/list-codes-effective-january-1-2020-published-december-2-2019',
'CPT_HCPCS_2019': 'https://www.cms.gov/apps/ama/license.asp?file=/Medicare/Fraud-and-Abuse/PhysicianSelfReferral/Downloads/2019-DHS-Addendum.zip',
'CPT_HCPCS_2018': 'https://www.cms.gov/apps/ama/license.asp?file=/Medicare/Fraud-and-Abuse/PhysicianSelfReferral/Downloads/2018-DHS-Addendum.zip',
'CPT_HCPCS_2017': 'https://www.cms.gov/apps/ama/license.asp?file=/Medicare/Fraud-and-Abuse/PhysicianSelfReferral/Downloads/2017-DHS-Addendum.zip',
'CPT_HCPCS_2016': 'https://www.cms.gov/apps/ama/license.asp?file=/Medicare/Fraud-and-Abuse/PhysicianSelfReferral/Downloads/2016-DHS-Addendum.zip',
'CPT_HCPCS_2015': 'https://www.cms.gov/apps/ama/license.asp?file=/Medicare/Fraud-and-Abuse/PhysicianSelfReferral/Downloads/2015-DHS-Addendum.zip',
'CPT_HCPCS_2014': 'https://www.cms.gov/apps/ama/license.asp?file=/Medicare/Fraud-and-Abuse/PhysicianSelfReferral/Downloads/2014-DHS-Addendum.zip',
'CPT_HCPCS_2013': 'https://www.cms.gov/apps/ama/license.asp?file=/Medicare/Fraud-and-Abuse/PhysicianSelfReferral/Downloads/2013-Addendum.zip',
'CPT_HCPCS_2012': 'https://www.cms.gov/apps/ama/license.asp?file=/Medicare/Fraud-and-Abuse/PhysicianSelfReferral/Downloads/2012Addendum.zip',
}


def zip_outputter(output_dir, dir_name, url):
    zip_output_dir = os.path.join(output_dir, dir_name)
    if os.path.exists(zip_output_dir):
        print('ZIP already extracted')
    else:
        r = requests.get(url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(zip_output_dir)
        print('Zip Extracted')


if __name__ == '__main__':

    for i in ['icd_cm_zip', 'icd_pcs_zip', 'cpt_hcpcs_zip']:
        try:
            os.mkdir(i)
        except FileExistsError:
            print('Directory already exists')
        except:
            raise

    output_dir = 'icd_cm_zip'

    for year, url in zip_icd_cm_file_urls.items():
        zip_outputter(output_dir, year, url)

    output_dir = 'icd_pcs_zip'

    for year, url in zip_icd_pcs_file_urls.items():
        zip_outputter(output_dir, year, url)

    output_dir = 'cpt_hcpcs_zip'
    for year, url in zip_cpt_file_urls.items():
        if os.path.exists(os.path.join(output_dir, year)):
            print('File previosuly  made')
        else: 
            os.mkdir(os.path.join(output_dir, year))