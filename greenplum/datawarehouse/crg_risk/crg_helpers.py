import pandas as pd
from datetime import date

def crg_enrl_yearly(cursor, data_source, start_age, end_age, year, use_fiscal_year=False):
    # Get list of enrolled people from given data source(s) for at least one calendar year

    query = f'''
drop table if exists dev.ip_{data_source}_crg_enrl_{year};

select uth_member_id, gender_cd, to_char(dob_derived, 'mmddyyyy') as dob, age_derived
into dev.ip_{data_source}_crg_enrl_{year}
from data_warehouse.member_enrollment_{'fiscal_' if use_fiscal_year else ''}yearly
where {'fiscal_' if use_fiscal_year else ''}year = {year}
and data_source = '{data_source}'
and age_derived between {start_age} and {end_age}
;
    '''

    cursor.execute(query)

    return cursor.rowcount

def crg_claim_detail(cursor, data_source, year, use_fiscal_year=False):
    ''' 
    Get the following from the claim detail table other than member id and claim id
        discharge status, 
        bill type code, 
        provider type
    '''

    query = f'''
    drop table if exists dev.ip_{data_source}_crg_claim_detail_{year};

    select distinct uth_member_id, uth_claim_id, 
            case 
                when discharge_status is null then '99'
                when discharge_status in ('NA','00','1','2','3','4','5','6','7','8','9',
                                            '0A','0C','0M','0P','0Y','+6',
                                            '96','97','98', 'C1','C5',
                                            'D1','DC','G0','OP','PC') then  '99' 
                else lpad(discharge_status,2,'0') 
            end as discharge_status,
            lpad(bill_type_inst || bill_type_class || bill_type_freq, 4, '0') as bill,
            case 
                when bill_type_inst is null then '2' 
                else '1'
            end as provider_type
    into dev.ip_{data_source}_crg_claim_detail_{year}
    from data_warehouse.claim_detail
    where {'fiscal_' if use_fiscal_year else ''}year = {year}
    and data_source = '{data_source}'
    ;
    '''

    cursor.execute(query)

    return cursor.rowcount

def crg_claim_pos(cursor, data_source, year, use_fiscal_year=False):
    ''' 
    Gets place of service code 
    '''

    query = f'''
    drop table if exists dev.ip_{data_source}_crg_claim_pos_{year};

    select uth_member_id, uth_claim_id, 
        min(
        case 
            when place_of_service = '1' and table_id_src = 'clm_detail' then '11'
            when place_of_service = '2' and table_id_src = 'clm_detail' then '12'
            when place_of_service = '3' and table_id_src = 'clm_detail' then '21'
            when place_of_service = '4' and table_id_src = 'clm_detail' then '31'
            when place_of_service = '5' and table_id_src = 'clm_detail' then '19'
            when place_of_service = '6' and table_id_src = 'clm_detail' then '81'
            when place_of_service = '1' and table_id_src = 'clm_detail' then '25'
            when place_of_service = '1' and table_id_src = 'clm_detail' then '32'
            when place_of_service is null or trim(place_of_service) in ('', '`', '?', '.', '\'\'') then '99'
            when place_of_service in ('NONE','UNK') then '99' 
	        when lpad(replace(place_of_service,'.',''),2) = '02' then '99'
            else place_of_service  
        end 
        )  as pos
    into dev.ip_{data_source}_crg_claim_pos_{year}
    from data_warehouse.claim_detail
    where {'fiscal_' if use_fiscal_year else ''}year = {year}
    and data_source = '{data_source}'
    group by 1,2
    ;
    '''

    cursor.execute(query)

    return cursor.rowcount

def crg_claim_dos(cursor, data_source, year, use_fiscal_year=False):
    ''' 
    Get all of the dates of service and concatenates them
    '''

    query = f'''
    drop table if exists dev.ip_{data_source}_crg_claim_dos_{year};

    select a.uth_member_id, a.uth_claim_id, 
        to_char(b.from_date_of_service, 'mmddyyyy') as from_date_of_service,
        to_char(b.to_date_of_service, 'mmddyyyy') as to_date_of_service,
        string_agg(to_char(a.from_date_of_service, 'mmddyyyy'), ';' order by claim_sequence_number) from_dos,
		string_agg(to_char(a.to_date_of_service, 'mmddyyyy'), ';' order by claim_sequence_number) to_dos
    into dev.ip_{data_source}_crg_claim_dos_{year}
    from (
        select distinct uth_member_id, uth_claim_id, 
            claim_sequence_number, from_date_of_service, to_date_of_service
        from data_warehouse.claim_detail
        where {'fiscal_' if use_fiscal_year else ''}year = {year}
        and data_source = '{data_source}'
    ) a
    join data_warehouse.claim_header b
    on a.uth_member_id = b.uth_member_id
    and a.uth_claim_id = b.uth_claim_id
    group by 1,2,3,4
    ;
    '''

    cursor.execute(query)

    return cursor.rowcount

def crg_claim_rev(cursor, data_source, year, use_fiscal_year=False):
    ''' 
    Get all of revenue codes and concatenates them
    '''

    query = f'''
    drop table if exists dev.ip_{data_source}_crg_claim_rev_{year};

    select uth_member_id, uth_claim_id, 
        string_agg(revenue_cd, ';' order by claim_sequence_number) revenue_cd
    into dev.ip_{data_source}_crg_claim_rev_{year}
    from (
        select distinct uth_member_id, uth_claim_id, 
            claim_sequence_number, revenue_cd
        from data_warehouse.claim_detail
        where {'fiscal_' if use_fiscal_year else ''}year = {year}
        and data_source = '{data_source}'
        and revenue_cd is not null
    ) a
    group by 1,2
    ;
    '''

    cursor.execute(query)

    return cursor.rowcount

def crg_claim_icd_proc(cursor, data_source, year, use_fiscal_year=False):
    query = f'''
    drop table if exists dev.ip_{data_source}_crg_icd_proc_{year};

    select uth_member_id, uth_claim_id, string_agg(proc_cd, ';') icd_proc
    into dev.ip_{data_source}_crg_icd_proc_{year}
    from (
        select distinct uth_member_id, uth_claim_id, proc_cd 
        from data_warehouse.claim_icd_proc
        where {'fiscal_' if use_fiscal_year else ''}year = {year}
        and data_source = '{data_source}'
    ) a
    group by uth_member_id, uth_claim_id
    ;
    '''

    cursor.execute(query)

    return cursor.rowcount

def crg_claim_proc(cursor, data_source, year, use_fiscal_year=False):
    query = f'''
    drop table if exists dev.ip_{data_source}_crg_proc_{year};

    select uth_member_id, uth_claim_id, string_agg(cpt_hcpcs_cd, ';') proc_cd
    into dev.ip_{data_source}_crg_proc_{year}
    from (
        select distinct uth_member_id, uth_claim_id, cpt_hcpcs_cd 
        from data_warehouse.claim_detail
        where {'fiscal_' if use_fiscal_year else ''}year = {year}
        and data_source = '{data_source}'
        and cpt_hcpcs_cd is not null
    ) a
    group by uth_member_id, uth_claim_id
    ;
    '''

    cursor.execute(query)

    return cursor.rowcount

def crg_claim_dx(cursor, data_source, year, use_fiscal_year=False):
    # Currently only trum and truc have 'P' diagnosis position, 
    # once all the other data sources have been updated to meet new mappings, this query will change again
    query = f'''
    drop table if exists dev.ip_{data_source}_crg_dx_{year};

    select a.*, b.secondary_dx
    into dev.ip_{data_source}_crg_dx_{year}
    from ( 
        select uth_member_id, uth_claim_id, diag_cd, icd_version
        from (
            select distinct uth_member_id, uth_claim_id, diag_cd, 
                    case 
                        when icd_version is null then dev.fn_crg_icd_version(icd_version, year) 
                        else icd_version 
                    end as icd_version
            from data_warehouse.claim_diag
            where {'fiscal_' if use_fiscal_year else ''}year = {year}
            and diag_position = '{'P' if data_source in ['trum', 'truc'] else '1'}'
            and data_source = '{data_source}'
        ) c
    ) a 
    left join (
        select uth_member_id, uth_claim_id, string_agg(diag_cd, ';' order by diag_position) as secondary_dx
        from (
            select distinct uth_member_id, uth_claim_id, diag_cd, diag_position
            from data_warehouse.claim_diag
            where {'fiscal_' if use_fiscal_year else ''}year = {year}
            and diag_position <> '{'P' if data_source in ['trum', 'truc'] else '1'}'
            and data_source = '{data_source}'
        ) a
        group by 1,2
    ) b
    on a.uth_member_id = b.uth_member_id
    and a.uth_claim_id = b.uth_claim_id;
    '''

    cursor.execute(query)

    return cursor.rowcount

def crg_admit(cursor, data_source, year):
    query =  f'''
    drop table if exists dev.ip_{data_source}_crg_admit_{year};

    select distinct a.uth_member_id, a.uth_claim_id, admit_date, discharge_date
    into dev.ip_{data_source}_crg_admit_{year}
    from (
        select distinct uth_member_id, uth_claim_id
        from dev.ip_{data_source}_crg_claim_detail_{year}
    ) a
    join data_warehouse.admission_acute_ip_claims b
    on a.uth_member_id = b.uth_member_id
    and a.uth_claim_id = b.uth_claim_id
    ;
    '''

    cursor.execute(query)

    return cursor.rowcount

def crg_input(cursor, data_source, year, use_fiscal_year=False):
    query = f'''
    drop table if exists dev.ip_{data_source}_crg_input_{'fy_' if use_fiscal_year else 'cy_'}{year};

    select distinct
            a.uth_member_id::text as PatientId,
            a.gender_cd as Sex,
            a.dob as BirthDate,
            b.uth_claim_id::text as ClaimId,
            coalesce(to_char(i.admit_date, 'mmddyyyy'), h.from_date_of_service) as AdmitDate,
		    coalesce(to_char(i.discharge_date, 'mmddyyyy'), h.to_date_of_service) AS DischargeDate,
            h.from_dos as ItemFromDate,
            h.to_dos as ItemToDate,
            b.discharge_status as DischargeStatus,
            case when b.bill is null then '' else b.bill end as TypeOfBill,
            g.pos as PlaceOfService,
            case when b.bill is null then 1 else 2 end as ProviderType,
            c.icd_version as ICDVersionQualifier,
            c.diag_cd as AdmitDiagnosis,
            c.diag_cd as PrimaryDiagnosis,
            c.secondary_dx as SecondaryDiagnosis,
            '' AS ExternalCauseOfInjuryDiagnosis,
            '' AS ReasonForVisitDiagnosis,
            case when d.icd_proc is null then '' else d.icd_proc end as "Procedure",
            '' as ProcedureDate,
            case when e.proc_cd is null then '' else e.proc_cd end as ProcedureHcpcs,
            '' AS ItemDiagnosisPointer,
            g.pos AS ItemPlaceOfService,
            case when b.bill is null then 1 else 2 end as ItemProviderType,
            '' AS ItemServiceDate,
            '' AS ItemNdcCode,
            '' AS ItemAtcCode,
            '' AS ItemDinCode,
            '' AS FunctionalStatusGrouperDisable,
            '' AS FunctionalStatusGrouperAssessmentDate,
            '' AS FunctionalStatusGrouperAssessmentTool,
            '' AS FunctionalStatusGrouperAssessmentItemId,
            '' AS FunctionalStatusGrouperAssessmentScore,
            f.revenue_cd AS ItemRevenueCode,
            '' AS ItemSiteOfService,
            '' AS SiteOfService
    into dev.ip_{data_source}_crg_input_{'fy_' if use_fiscal_year else 'cy_'}{year}
    from dev.ip_{data_source}_crg_enrl_{year} a
    inner join dev.ip_{data_source}_crg_claim_detail_{year} b
    on a.uth_member_id = b.uth_member_id
    left join dev.ip_{data_source}_crg_dx_{year} c
    on b.uth_member_id = c.uth_member_id
    and b.uth_claim_id = c.uth_claim_id
    left join dev.ip_{data_source}_crg_icd_proc_{year} d
    on b.uth_member_id = d.uth_member_id
    and b.uth_claim_id = d.uth_claim_id
    left join dev.ip_{data_source}_crg_proc_{year} e
    on b.uth_member_id = e.uth_member_id
    and b.uth_claim_id = e.uth_claim_id
    left join dev.ip_{data_source}_crg_claim_rev_{year} f
    on b.uth_member_id = f.uth_member_id
    and b.uth_claim_id = f.uth_claim_id
    left join dev.ip_{data_source}_crg_claim_pos_{year} g
    on b.uth_member_id = g.uth_member_id
    and b.uth_claim_id = g.uth_claim_id
    join dev.ip_{data_source}_crg_claim_dos_{year} h
    on b.uth_member_id = h.uth_member_id
    and b.uth_claim_id = h.uth_claim_id
    left join dev.ip_{data_source}_crg_admit_{year} i
    on b.uth_member_id = i.uth_member_id
    and b.uth_claim_id = i.uth_claim_id
    ;
    '''

    cursor.execute(query)
    row_count = cursor.rowcount

    cursor.execute(f'''vacuum analyze dev.ip_{data_source}_crg_input_{'fy_' if use_fiscal_year else 'cy_'}{year};''')
    return row_count

def swap_uth_id_src_id(cursor, data_source, year, use_fiscal_year=False):
    query = f'''
    update dev.ip_{data_source}_crg_input_{'fy_' if use_fiscal_year else 'cy_'}{year} a
    set patientid = b.member_id_src,
        claimid = b.claim_id_src
    from (
        select *
        from data_warehouse.dim_uth_claim_id
        where data_source = '{data_source}'
        and data_year between {year}-1 and {year}
    ) b
    where a.patientid = b.uth_member_id::text
    and a.claimid = b.uth_claim_id::text
    '''
    
    cursor.execute(query)

def generate_crg_input_table(cursor, data_source, year, start_age=None, end_age=None, use_fiscal_year=False, user_src_id=False):
    if start_age is not None and end_age is not None:
        print('Enrollment: ', crg_enrl_yearly(cursor, data_source, start_age, end_age, year, use_fiscal_year))
        print('Input: ', crg_input(cursor, data_source, year, use_fiscal_year))
    else:
        print('Claim Detail: ', crg_claim_detail(cursor, data_source, year, use_fiscal_year))
        print('Claim POS: ', crg_claim_pos(cursor, data_source, year, use_fiscal_year))
        print('Claim DOS: ', crg_claim_dos(cursor, data_source, year, use_fiscal_year))
        print('Claim Revenue Codes: ', crg_claim_rev(cursor, data_source, year, use_fiscal_year))
        print('ICD Proc: ', crg_claim_icd_proc(cursor, data_source, year, use_fiscal_year))
        print('Proc: ', crg_claim_proc(cursor, data_source, year, use_fiscal_year))
        print('DX: ', crg_claim_dx(cursor, data_source, year, use_fiscal_year))
        print('Admit: ', crg_admit(cursor, data_source, year))

    if user_src_id:
        print('Swapping uth ids with source ids')
        swap_uth_id_src_id(cursor, data_source, year, use_fiscal_year)

def crg_input_file_batch_export(cursor, year, data_source, start_age, end_age, file_directory, use_fiscal_year=False):
    # Creates partitions based on people age from the input table so that 3M can run with no issues
    with open(file_directory+f'''{data_source}_input_crg_{'fy_' if use_fiscal_year else 'cy_'}{year}_{start_age}_{end_age}.csv''', 'w') as file:
        query = f'''
        select * 
        from dev.ip_{data_source}_crg_input_{'fy_' if use_fiscal_year else 'cy_'}{year}
        order by PatientId, ItemFromDate'''
        query = f'''copy ({query}) to stdout with csv header'''
        cursor.copy_expert(query, file)

def create_crg_table(cursor, schema, table_name):
    cursor.execute(
        f'''
drop table if exists {schema}.{table_name};

create table {schema}.{table_name}
(
    data_source bpchar(4),
    uth_member_id int8,
    crg_year int2,
    crg text,
    aggregated_crg_3 text,
    prospective_crg	text,
    prospective_agg_crg_1 text,
    prospective_agg_crg_2 text,
    prospective_agg_crg_3 text,
    concurrent_crg text,
    concurrent_agg_crg_1 text,
    concurrent_agg_crg_2 text,
    concurrent_agg_crg_3 text,
    load_date date
)
WITH (
    appendonly=true,
    orientation=column,
    compresstype=zlib
)
DISTRIBUTED BY (uth_member_id, crg_year);
        '''
    )
    return -1

def crg_read_csv(crg_file, data_source, crg_year):
    
    # Once 3M generates the file with the CRG scores, read the relevant columns as a dataframe
    try:
        crg_df = pd.read_csv(crg_file,
                            dtype={'Crg': 'str',
                                    'AggregatedCrg3': 'str',
                                    'ProspectiveCrg': 'str',
                                    'ProspectiveAggregatedCrg1': 'str',
                                    'ProspectiveAggregatedCrg2': 'str',
                                    'ProspectiveAggregatedCrg23': 'str',
                                    'ConcurrentCrg': 'str',
                                    'ConcurrentAggregatedCrg1': 'str',
                                    'ConcurrentAggregatedCrg2': 'str',
                                    'ConcurrentAggregatedCrg2': 'str'},
                            usecols=['PatientId',
                                    'Crg',
                                    'AggregatedCrg3',
                                    'ProspectiveCrg',
                                    'ProspectiveAggregatedCrg1',
                                    'ProspectiveAggregatedCrg2',
                                    'ProspectiveAggregatedCrg3',
                                    'ConcurrentCrg', 'ConcurrentAggregatedCrg1',
                                    'ConcurrentAggregatedCrg2',
                                    'ConcurrentAggregatedCrg3'])
    except:
        # If 3M generated an empty output file, skip the rest of the process and ignore having to upload to DB
        return None

    crg_df.loc[:, 'data_source'] = data_source
    crg_df.loc[:, 'crg_year'] = crg_year
    crg_df.loc[:, 'load_date'] = date.today()

    crg_df = crg_df.rename(columns={'PatientId': 'uth_member_id',
                                    'Crg': 'crg',
                                    'AggregatedCrg3': 'aggregated_crg_3',
                                    'ProspectiveCrg': 'prospective_crg',
                                    'ProspectiveAggregatedCrg1': 'prospective_agg_crg_1',
                                    'ProspectiveAggregatedCrg2': 'prospective_agg_crg_2',
                                    'ProspectiveAggregatedCrg3': 'prospective_agg_crg_3',
                                    'ConcurrentCrg': 'concurrent_crg',
                                    'ConcurrentAggregatedCrg1': 'concurrent_agg_crg_1',
                                    'ConcurrentAggregatedCrg2': 'concurrent_agg_crg_2',
                                    'ConcurrentAggregatedCrg3': 'concurrent_agg_crg_3'
                                    })

    crg_df = crg_df[['data_source', 'uth_member_id', 'crg_year',
                     'crg', 'aggregated_crg_3',
                     'prospective_crg', 'prospective_agg_crg_1',
                     'prospective_agg_crg_2', 'prospective_agg_crg_3',
                     'concurrent_crg', 'concurrent_agg_crg_1', 
                     'concurrent_agg_crg_2', 'concurrent_agg_crg_3', 'load_date']]

    crg_df = crg_df.dropna(subset=['uth_member_id'])
    crg_df['uth_member_id'] = crg_df['uth_member_id'].astype(int)

    return crg_df

def drop_intermediate_tables(cursor, data_source, year, use_fiscal_year=False):
    query = f'''
    drop table if exists dev.ip_{data_source}_crg_input_{'fy_' if use_fiscal_year else 'cy_'}{year};
    drop table if exists dev.ip_{data_source}_crg_enrl_{year};
    drop table if exists dev.ip_{data_source}_crg_claim_detail_{year};
    drop table if exists dev.ip_{data_source}_crg_dx_{year};
    drop table if exists dev.ip_{data_source}_crg_icd_proc_{year};
    drop table if exists dev.ip_{data_source}_crg_proc_{year};
    drop table if exists dev.ip_{data_source}_crg_claim_rev_{year};
    drop table if exists dev.ip_{data_source}_crg_claim_pos_{year};
    drop table if exists dev.ip_{data_source}_crg_claim_dos_{year};
    drop table if exists dev.ip_{data_source}_crg_admit_{year};
    '''

    cursor.execute(query)