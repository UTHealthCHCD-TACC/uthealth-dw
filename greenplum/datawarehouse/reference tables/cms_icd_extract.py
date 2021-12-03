#must have the zip files from cms as seen in cms_icd_zip_getter.py
#note in 2020 through 2022 ensure all files are directly below the directory
#in order to be read

import re
import csv
import pandas as pd


def cms_cd_line_cleaner(line, constant_dict, slice_dict):
    line_dict = {}
    line_dict.update(constant_dict)
    for key, value in slice_dict.items():
        sliced_line = line[value]
        if type(sliced_line) == list:
            sliced_line = sliced_line[0]
        line_dict[key] = sliced_line.strip()
    return line_dict

def cms_cd_file_reader(file_path, constant_dict={}, slice_dict={}, grep_pattern=None):

    output_list = []
    with open(file_path) as f:
        if file_path.endswith('.csv'):
            f = csv.reader(f)
            next(f)
        for line in f:
            if grep_pattern is not None:
                if re.match(grep_pattern, line) is None:
                    continue
                else:
                    line_dict = cms_cd_line_cleaner(line, constant_dict, slice_dict)
                    output_list.append(line_dict)
            else:
                line_dict = cms_cd_line_cleaner(line, constant_dict, slice_dict)
                output_list.append(line_dict)
    return output_list


if __name__ == '__main__':

    code_files = [
    {'file_path': 'icd_cm_zip\\ICD10_2022\\icd10cm_order_2022.txt',
    'cd_type': 'ICD10-CM',
    'cd_year': 2022},
    {'file_path': 'icd_cm_zip\\ICD10_2021\\icd10cm_order_2021.txt',
    'cd_type': 'ICD10-CM',
    'cd_year': 2021},
    {'file_path': 'icd_cm_zip\\ICD10_2020\\icd10cm_order_2020.txt',
    'cd_type': 'ICD10-CM',
    'cd_year': 2020},
    {'file_path': 'icd_cm_zip\\ICD10_2019\\icd10cm_order_2019.txt',
    'cd_type': 'ICD10-CM',
    'cd_year': 2019},
    {'file_path': 'icd_cm_zip\\ICD10_2018\\icd10cm_order_2018.txt',
    'cd_type': 'ICD10-CM',
    'cd_year': 2018},
    {'file_path': 'icd_cm_zip\\ICD10_2017\\icd10cm_order_2017.txt',
    'cd_type': 'ICD10-CM',
    'cd_year': 2017},
    {'file_path': 'icd_cm_zip\\ICD10_2016\\icd10cm_order_2016.txt',
    'cd_type': 'ICD10-CM',
    'cd_year': 2016},
    {'file_path': 'icd_cm_zip\\ICD10_2015\\icd10cm_order_2015.txt',
    'cd_type': 'ICD10-CM',
    'cd_year': 2015},
    {'file_path': 'icd_cm_zip\\ICD10_2014\\icd10cm_order_2014.txt',
    'cd_type': 'ICD10-CM',
    'cd_year': 2014},
    {'file_path': 'icd_cm_zip\\ICD9_2014\\CMS32_DESC_SHORT_DX.txt',
    'cd_type': 'ICD9-CM',
    'cd_year': 2014},
    {'file_path': 'icd_cm_zip\\ICD9_2013\\CMS31_DESC_SHORT_DX.txt',
    'cd_type': 'ICD9-CM',
    'cd_year': 2013},
    {'file_path': 'icd_cm_zip\\ICD9_2012\\CMS30_DESC_SHORT_DX.txt',
    'cd_type': 'ICD9-CM',
    'cd_year': 2012},
    {'file_path': 'icd_cm_zip\\ICD9_2011\\CMS29_DESC_SHORT_DX.txt',
    'cd_type': 'ICD9-CM',
    'cd_year': 2011},
    {'file_path': 'icd_cm_zip\\ICD9_2010\\CMS28_DESC_SHORT_DX.txt',
    'cd_type': 'ICD9-CM',
    'cd_year': 2010},
    {'file_path': 'icd_cm_zip\\ICD9_2009\\V27LONG_SHORT_DX_110909.csv',
    'cd_type': 'ICD9-CM',
    'cd_year': 2009},
    {'file_path': 'icd_cm_zip\\ICD9_2008\\V26 I-9 Diagnosis.txt',
    'cd_type': 'ICD9-CM',
    'cd_year': 2008},
    {'file_path': 'icd_cm_zip\\ICD9_2007\\I9diagnosesV25.txt',
    'cd_type': 'ICD9-CM',
    'cd_year': 2007},
    {'file_path': 'icd_cm_zip\\ICD9_2006\\I9diagnosis.txt',
    'cd_type': 'ICD9-CM',
    'cd_year': 2006},
    {'file_path': 'icd_cm_zip\\ICD9_2005\\I9DX_DESC.txt',
    'cd_type': 'ICD9-CM',
    'cd_year': 2005},
    {'file_path': 'icd_pcs_zip\\ICD10_2022\\icd10pcs_order_2022.txt',
    'cd_type': 'ICD10-PCS',
    'cd_year': 2022},
    {'file_path': 'icd_pcs_zip\\ICD10_2021\\icd10pcs_order_2021.txt',
    'cd_type': 'ICD10-PCS',
    'cd_year': 2021},
    {'file_path': 'icd_pcs_zip\\ICD10_2020\\icd10pcs_order_2019.txt',
    'cd_type': 'ICD10-PCS',
    'cd_year': 2020},
    {'file_path': 'icd_pcs_zip\\ICD10_2019\\icd10pcs_order_2019.txt',
    'cd_type': 'ICD10-PCS',
    'cd_year': 2019},
    {'file_path': 'icd_pcs_zip\\ICD10_2018\\icd10pcs_order_2018.txt',
    'cd_type': 'ICD10-PCS',
    'cd_year': 2018},
    {'file_path': 'icd_pcs_zip\\ICD10_2017\\icd10pcs_order_2017.txt',
    'cd_type': 'ICD10-PCS',
    'cd_year': 2017},
    {'file_path': 'icd_pcs_zip\\ICD10_2016\\icd10pcs_order_2016.txt',
    'cd_type': 'ICD10-PCS',
    'cd_year': 2016},
    {'file_path': 'icd_pcs_zip\\ICD10_2015\\icd10pcs_order_2015.txt',
    'cd_type': 'ICD10-PCS',
    'cd_year': 2015},
    {'file_path': 'icd_pcs_zip\\ICD10_2014\\icd10pcs_order_2014.txt',
    'cd_type': 'ICD10-PCS',
    'cd_year': 2014},
    {'file_path': 'icd_pcs_zip\\ICD9_2014\\CMS32_DESC_SHORT_SG.txt',
    'cd_type': 'ICD9-PCS',
    'cd_year': 2014},
    {'file_path': 'icd_pcs_zip\\ICD9_2013\\CMS31_DESC_SHORT_SG.txt',
    'cd_type': 'ICD9-PCS',
    'cd_year': 2013},
    {'file_path': 'icd_pcs_zip\\ICD9_2012\\CMS30_DESC_SHORT_SG.txt',
    'cd_type': 'ICD9-PCS',
    'cd_year': 2012},
    {'file_path': 'icd_pcs_zip\\ICD9_2011\\CMS29_DESC_SHORT_SG.txt',
    'cd_type': 'ICD9-PCS',
    'cd_year': 2011},
    {'file_path': 'icd_pcs_zip\\ICD9_2010\\CMS28_DESC_SHORT_SG.txt',
    'cd_type': 'ICD9-PCS',
    'cd_year': 2010},
    {'file_path': 'icd_pcs_zip\\ICD9_2009\\CMS27_DESC_LONG_SHORT_SG_092709.csv',
    'cd_type': 'ICD9-PCS',
    'cd_year': 2009},
    {'file_path': 'icd_pcs_zip\\ICD9_2008\\V26  I-9 Procedures.txt',
    'cd_type': 'ICD9-PCS',
    'cd_year': 2008},
    {'file_path': 'icd_pcs_zip\\ICD9_2007\\I9proceduresV25.txt',
    'cd_type': 'ICD9-PCS',
    'cd_year': 2007},
    {'file_path': 'icd_pcs_zip\\ICD9_2006\\I9surgery.txt',
    'cd_type': 'ICD9-PCS',
    'cd_year': 2006},
    {'file_path': 'icd_pcs_zip\\ICD9_2005\\I9SG_DESC.txt',
    'cd_type': 'ICD9-PCS',
    'cd_year': 2005},
    {'file_path': 'cpt_hcpcs_zip\\CPT_HCPCS_2022\\Section 508 Version Compliant of 2022_DHS_Code_List_Addendum_10_28_21.txt',
    'cd_type': 'CPT-HCPCS',
    'cd_year': 2022},
    {'file_path': 'cpt_hcpcs_zip\\CPT_HCPCS_2020\\508-compliant-version-of-2020dhsaddendum.txt',
    'cd_type': 'CPT-HCPCS',
    'cd_year': 2020},
    {'file_path': 'cpt_hcpcs_zip\\CPT_HCPCS_2019\\2019dhsaddendum.txt',
    'cd_type': 'CPT-HCPCS',
    'cd_year': 2019},
    {'file_path': 'cpt_hcpcs_zip\\CPT_HCPCS_2018\\2018dhsaddendum_10_24_17final.txt',
    'cd_type': 'CPT-HCPCS',
    'cd_year': 2018},
    {'file_path': 'cpt_hcpcs_zip\\CPT_HCPCS_2017\\2017dhsaddendum_final_10_26_16.txt',
    'cd_type': 'CPT-HCPCS',
    'cd_year': 2017},
    {'file_path': 'cpt_hcpcs_zip\\CPT_HCPCS_2016\\2016DHSAddendum_10-16-15.txt',
    'cd_type': 'CPT-HCPCS',
    'cd_year': 2016},
    {'file_path': 'cpt_hcpcs_zip\\CPT_HCPCS_2015\\2015DHSAddendum_10-30-14.txt',
    'cd_type': 'CPT-HCPCS',
    'cd_year': 2015},
    {'file_path': 'cpt_hcpcs_zip\\CPT_HCPCS_2014\\2014DHSAddendum.txt',
    'cd_type': 'CPT-HCPCS',
    'cd_year': 2014},
    {'file_path': 'cpt_hcpcs_zip\\CPT_HCPCS_2013\\Copy of 2013Addendum.txt',
    'cd_type': 'CPT-HCPCS',
    'cd_year': 2013},
    {'file_path': 'cpt_hcpcs_zip\\CPT_HCPCS_2012\\2012AddendumwCorrections.txt',
    'cd_type': 'CPT-HCPCS',
    'cd_year': 2012}]
    
    icd_9_cm_txt_slice_dict = {'cd_value':slice(0,5),'code_description':slice(6,None)}
    icd_9_cm_csv_slice_dict = {'cd_value':slice(1),'code_description':slice(2,3)}
    icd_10_cm_txt_slice_dict = {'cd_value':slice(6,13),'code_description':slice(16,76)}


    icd_9_pcs_txt_slice_dict = {'cd_value':slice(0,4),'code_description':slice(5,None)}
    icd_9_pcs_csv_slice_dict = {'cd_value':slice(1),'code_description':slice(2,3)}
    icd_10_pcs_txt_slice_dict = {'cd_value':slice(6,13),'code_description':slice(16,76)}


    cpt_txt_slice_dict = {'cd_value':slice(0,5),'code_description':slice(6,None)}
    grep_pattern = "^\w{5}\t"

    for file_dict in code_files:
        file_dict['constant_dict'] = {'cd_year': file_dict['cd_year'], 'cd_type':file_dict['cd_type']}
        file_dict['grep_pattern'] = None

        if file_dict['cd_type'] == 'ICD10-CM':
            file_dict['slice_dict'] = icd_10_cm_txt_slice_dict
            
        elif file_dict['cd_type'] == 'ICD9-CM':
            if file_dict['file_path'].endswith('.csv'):
                file_dict['slice_dict'] = icd_9_cm_csv_slice_dict
            else:
                file_dict['slice_dict'] = icd_9_cm_txt_slice_dict
                
        elif file_dict['cd_type'] == 'ICD10-PCS':
            file_dict['slice_dict'] = icd_10_pcs_txt_slice_dict
            
        elif file_dict['cd_type'] == 'ICD9-PCS':
            if file_dict['file_path'].endswith('.csv'):
                file_dict['slice_dict'] = icd_9_pcs_csv_slice_dict
            else:
                file_dict['slice_dict'] = icd_9_pcs_txt_slice_dict
                
        elif file_dict['cd_type'] == 'CPT-HCPCS':
            file_dict['slice_dict'] = cpt_txt_slice_dict
            file_dict['grep_pattern'] = "^\w{5}\t"

        total_codes = []

        for code_file_dict in code_files:
            
            output_data = cms_cd_file_reader(code_file_dict['file_path'], code_file_dict['constant_dict'], 
                                            code_file_dict['slice_dict'], grep_pattern=code_file_dict['grep_pattern'])
            total_codes = [*total_codes, *output_data]
            

        total_codes_df = pd.DataFrame(total_codes)

        total_codes_df = total_codes_df.groupby(['cd_type', 'cd_value']).agg(initial_year=('cd_year','min'),
                                                                                    last_year=('cd_year','max'), 
                                                                                    code_description=('code_description', 'first'))
        total_codes_df.to_csv('./cms_code_output/cms_codes.csv')