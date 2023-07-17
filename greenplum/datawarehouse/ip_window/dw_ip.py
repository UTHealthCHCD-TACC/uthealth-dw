from datetime import datetime
import pandas as pd
import psycopg2
import psycopg2.extras
import sys
sys.path.append('H:/chcd_py')
from chcd_py.helpers.sql_logger import db_logger, std_out_logger, pipeline_runner, create_log_table
from chcd_py.helpers.db_utils import get_dsn, io_copy_from
from chcd_py.ip_window.dw_ip_window_step_2 import ip_window_wrapper
log_name = 'dev.ip_dw_ip_log'

@std_out_logger
@db_logger(log_name)
def ip_acute_drop_tables(cursor, **kwargs):
    '''drops tables'''
    cursor.execute("""
    drop table if exists dev.gm_dw_ip_window_step_1;
    drop table if exists dev.gm_dw_ip_window_step_2;
    DROP TABLE if exists dev.gm_dw_ip_admit;
    drop table if exists dev.gm_dw_ip_admit_claim;""")
    return -1




@std_out_logger
@db_logger(log_name)
def ip_acute_create_tables(cursor, create_dw_table=False, **kwargs):
    '''Drops and creates tables'''
    cursor.execute("""
create table dev.gm_dw_ip_window_step_1  (
data_source bpchar(4),
uth_member_id int8,
uth_claim_id int8,
admit_date date,
discharge_date date,
from_date_of_service date,
to_date_of_service date,
discharge_status varchar,
bill_type varchar,
insert_ts timestamp(0) DEFAULT LOCALTIMESTAMP)
with (appendonly=true,orientation=column) distributed by (uth_member_id) ;

create table dev.gm_dw_ip_window_step_2  (
data_source bpchar(4),
uth_member_id int8,
uth_claim_id int8,
admit_date date,
discharge_date date,
discharge_status varchar,
bill_type varchar,
pat_group int,
insert_ts timestamp(0) DEFAULT LOCALTIMESTAMP
)
with (appendonly=true,orientation=column) distributed by (uth_member_id) ;


CREATE UNLOGGED TABLE dev.gm_dw_ip_admit (
    data_source bpchar(4),
    uth_member_id int8 NULL,
    enc_id int4 NULL,
    admit_date date NULL,
    discharge_date date NULL,
    enc_discharge_status varchar null,
    admit_id varchar null,
    insert_ts timestamp(0) DEFAULT LOCALTIMESTAMP
)
WITH (
    appendonly=true,
    orientation=column,
    compresstype=zlib
)
DISTRIBUTED BY (uth_member_id);


CREATE UNLOGGED TABLE dev.gm_dw_ip_admit_claim (
    data_source bpchar(4),
    admit_id varchar,
    uth_member_id int8 NULL,
    enc_id int4 NULL,
    admit_date date NULL,
    discharge_date date NULL,
    enc_discharge_status varchar null,
    uth_claim_id varchar,
    from_date_of_service date,
    to_date_of_service date,
    claim_type varchar null,
    insert_ts timestamp(0) DEFAULT LOCALTIMESTAMP
)
WITH (
    appendonly=true,
    orientation=column,
    compresstype=zlib
)
DISTRIBUTED BY (admit_id);""")
    
    if create_dw_table:
        cursor.execute('''
drop table if exists data_warehouse.admission_acute_ip;

create table data_warehouse.admission_acute_ip 
(
data_source bpchar(4),
year int2,
derived_uth_admission_id varchar,
uth_member_id int8 null,
admit_date date null,
discharge_date date,
admission_days int null,
discharge_status varchar,
primary_diagnosis_cd text  null,
primary_icd_proc_cd text  null,
total_charge_amount numeric(13,2)  null,
total_allowed_amount numeric(13,2)  null,
total_paid_amount numeric(13,2)  null,
days_to_readmit int null,
readmit_30 smallint,
member_id_src text,
insert_ts timestamp(0)
) distributed by (uth_member_id);

alter table data_warehouse.admission_acute_ip owner to uthealth_dev;

drop table if exists data_warehouse.admission_acute_ip_claims;

create table data_warehouse.admission_acute_ip_claims
(
data_source bpchar(4),
year int2,
derived_uth_admission_id varchar,
uth_member_id int8 null,
enc_id int, 
admit_date date null,
discharge_date date,
discharge_status varchar,
uth_claim_id int8,
from_date_of_service date null,
to_date_of_service date,
claim_type varchar,
charge_amount numeric(13,2)  null,
allowed_amount numeric(13,2)  null,
paid_amount numeric(13,2)  null,
member_id_src text,
claim_id_src text,
insert_ts timestamp(0)
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zlib
)
DISTRIBUTED BY (derived_uth_admission_id);

alter table data_warehouse.admission_acute_ip_claims owner to uthealth_dev;

        ''')
    
    return cursor.rowcount


@std_out_logger
@db_logger(log_name)
def ip_acute_insert_step_1(cursor, start_year, end_year, **kwargs):
    '''insert acute data claims'''
    cursor.execute("""
with ip_data as (select
    data_source,
    uth_member_id,
    uth_claim_id ,
    claim_sequence_number,
    from_date_of_service,
    to_date_of_service,
    admit_date,
    discharge_date,
    discharge_status,
    concat(bill_type_inst, bill_type_class, bill_type_freq) as bill_type
from
    data_warehouse.claim_detail ch
where
    bill in ('111', '114')
    and year between %s and %s)
insert into dev.gm_dw_ip_window_step_1
select
    data_source,
    uth_member_id,
    uth_claim_id,
    min(from_date_of_service) as admit_date,
    max(to_date_of_service) discharge_date,
    min(from_date_of_service) as from_date_of_service,
    max(to_date_of_service) to_date_of_service,
    discharge_status,
    bill_type from ip_data
group by data_source, uth_member_id, uth_claim_id,
discharge_status, bill_type;""",
     (start_year, end_year))
    return cursor.rowcount


@std_out_logger
@db_logger(log_name)
def ip_acute_delete_step_1(cursor, start_year, end_year, **kwargs):
    cursor.execute('delete from dev.gm_dw_ip_window_step_1 where to_date_of_service is null;')
    return cursor.rowcount


@std_out_logger
@db_logger(log_name)
def ip_acute_insert_step_2(cursor, **kwargs):
    cursor.execute('''
insert into dev.gm_dw_ip_window_step_2(
    data_source,
    uth_member_id,
    uth_claim_id ,
    admit_date,
    discharge_date,
    discharge_status,
    bill_type,
    pat_group)
with all_inp as (
select
    data_source,
    uth_member_id,
    uth_claim_id,
    min(admit_date) as admit_date,
    max(discharge_date) as discharge_date,
    discharge_status,
    bill_type,
    dense_rank() over (order by data_source, uth_member_id) / 1000000 pat_group
from
    dev.gm_dw_ip_window_step_1
group by
    data_source,
    uth_member_id,
    uth_claim_id,
    bill_type,
    discharge_status
)
select
    distinct ip.data_source,
    ip.uth_member_id,
    ip.uth_claim_id,
    ac.admit_date,
    ac.discharge_date,
    ac.discharge_status,
    ac.bill_type,
    ip.pat_group
from
    all_inp ip
join dev.gm_dw_ip_window_step_1 ac on
    ip.uth_member_id = ac.uth_member_id and ip.uth_claim_id = ac.uth_claim_id
where
    ac.admit_date between ip.admit_date and ip.discharge_date;''')
    return cursor.rowcount

def run_step_one():
    # step one; run sql
    acute_ip_pipeline = [ip_acute_drop_tables,
                         ip_acute_create_tables,
                         ip_acute_insert_step_1,
                         ip_acute_delete_step_1,
                         ip_acute_insert_step_2]

    variable_dict = {'start_year': 2015,
                     'end_year': 2022}

    sequence_description = 'IP Acute Admit: Step One'

    pipeline_runner(acute_ip_pipeline, sequence_description, variable_dict)

@std_out_logger
@db_logger(log_name)
def ip_window_by_group(cursor, df_con, data_source, pat_group, output_table, **kwargs):
    cursor.execute('''delete from dev.gm_dw_ip_admit_claim;''')

    sql_string = f'''select * from dev.gm_dw_ip_window_step_2 where
                        data_source = '{data_source}' and
                        pat_group = {pat_group};'''
    clm_df = pd.read_sql(sql_string, con=df_con,
                         parse_dates=['admit_date', 'discharge_date'])
    final_ip_group = ip_window_wrapper(clm_df)
    final_ip_group.loc[:, 'insert_ts'] = datetime.now()
    row_count = io_copy_from(df_con, final_ip_group, 'dev', output_table)
    return row_count

def run_step_two(variable_dict):

    step_two_pipeline = [ip_window_by_group]
    sequence_description = f'IP Acute Admit: Step Two ({variable_dict["data_source"]} {variable_dict["pat_group"]})'
    print('running: {}'.format(sequence_description))
    pipeline_runner(step_two_pipeline, sequence_description, variable_dict)
    print('completed', end=': ')
    print(sequence_description)

# start of step three pipeline
@std_out_logger
@db_logger(log_name)
def insert_ip_admit_claims(cursor, **kwargs):
    '''inserts all claims that occur during the ip_window timeframe'''

    cursor.execute(f'''delete from dev.gm_dw_ip_admit_claim;''')

    cursor.execute('''
    insert
        into
        dev.gm_dw_ip_admit_claim 
    (data_source,
        admit_id,
        uth_member_id,
        enc_id,
        admit_date,
        discharge_date,
        enc_discharge_status ,
        uth_claim_id,
        from_date_of_service,
        to_date_of_service,
        claim_type)
    select
        dia.data_source,
        admit_id,
        dia.uth_member_id,
        enc_id,
        admit_date,
        discharge_date,
        enc_discharge_status,
        uth_claim_id,
        from_date_of_service,
        to_date_of_service,
        claim_type
    from
        dev.gm_dw_ip_admit dia
    inner join data_warehouse.claim_header ch 
    on
        dia.uth_member_id = ch.uth_member_id
        and 
    (from_date_of_service between admit_date and discharge_date)
        and
    (to_date_of_service between admit_date and discharge_date);''')

    return cursor.rowcount

@std_out_logger
@db_logger(log_name)
def dw_import(cursor, data_source, **kwargs):
    '''moves the data from dev to data warehouse; 
    also calculates 30 day readmission'''

    cursor.execute(f'''delete from data_warehouse.admission_acute_ip where data_source = '{data_source}';''')

    cursor.execute(f'''
    insert
        into
        data_warehouse.admission_acute_ip (data_source,
        year,
        derived_uth_admission_id,
        uth_member_id,
        admit_date,
        discharge_date,
        admission_days,
        discharge_status,
        days_to_readmit,
        readmit_30,
        insert_ts)
    select
        data_source,
        extract(year from admit_date),
        admit_id,
        uth_member_id,
        admit_date,
        discharge_date,
        case when discharge_date-admit_date = 0 then 1
        else discharge_date-admit_date
        end,
        enc_discharge_status,
        admit_date - lag (discharge_date) over ( partition by uth_member_id order by admit_date) as days_to_readmit,
    case when admit_date - lag (discharge_date) over ( partition by uth_member_id order by admit_date)<=30 then 1
    else 0
    end as readmit_30,
    insert_ts
    from
        dev.gm_dw_ip_admit
    where data_source = '{data_source}'
    order by uth_member_id, admit_date;''')

    return cursor.rowcount

@std_out_logger
@db_logger(log_name)
def dw_import_claims(cursor, data_source, **kwargs):
    '''moves the claim data from dev to data warehouse; 
    '''

    cursor.execute(f'''delete from data_warehouse.admission_acute_ip_claims where data_source = '{data_source}';''')

    cursor.execute(f'''
    insert
        into
        data_warehouse.admission_acute_ip_claims (
        data_source,
        year,
        derived_uth_admission_id,
        uth_member_id,
        enc_id,
        admit_date,
        discharge_date,
        discharge_status,
        uth_claim_id,
        from_date_of_service,
        to_date_of_service,
        claim_type,
        charge_amount,
        allowed_amount,
        paid_amount,
        insert_ts
    )
    select
        data_source,
        extract(year from admit_date),
        admit_id,
        uth_member_id,
        enc_id,
        admit_date,
        discharge_date,
        enc_discharge_status,
        uth_claim_id,
        from_date_of_service,
        to_date_of_service,
        claim_type,
        charge_amount,
        allowed_amount,
        paid_amount,
    insert_ts
    from
        dev.gm_dw_ip_admit_claim
    where data_source = '{data_source}'
    order by uth_member_id, admit_date;''')

    return cursor.rowcount

def run_step_three(variable_dict):

    step_three_pipeline = [insert_ip_admit_claims, dw_import, dw_import_claims] #, admit_costs_update]
    sequence_description = f'IP Acute Admit: Step Three {variable_dict["data_source"]}'
    print('running: {}'.format(sequence_description))
    pipeline_runner(step_three_pipeline, sequence_description, variable_dict)
    print('completed', end=': ')
    print(sequence_description)

if __name__ == '__main__':
    df_con = psycopg2.connect(get_dsn())
    df_con.autocommit = True
    create_log_table(df_con.cursor(), 'dev', 'ip_dw_ip_log')
    df_con.close()

    # clears tables; and inserts all
    # inpatient claims; adds a group identifier
    run_step_one()

    # step two: runs the python logic
    # see script dw_ip_window_step_2.py
    output_table = 'gm_dw_ip_admit'

    try:

        # iterates through the groups
        # allows for processing in memory
        df_con = psycopg2.connect(get_dsn())
        df_con.autocommit = True
        with df_con.cursor() as cursor:
            
            cur = df_con.cursor()
            cur.execute(f'''select distinct data_source, pat_group
                        from dev.gm_dw_ip_window_step_2
                        where data_source in ('mcrt', 'mcrn')
                        order by data_source, pat_group
                        ;''')
            results = cur.fetchall()
            print(results)

        for pat_group in results:
            variable_dict = {'df_con': df_con, 'data_source': pat_group[0],
                             'pat_group': pat_group[1], 
                             'output_table': output_table}
            run_step_two(variable_dict)
            run_step_three(variable_dict)

    except:
        raise
    finally:
        df_con.close()