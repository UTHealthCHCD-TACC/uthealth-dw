from datetime import datetime
import pandas as pd
import psycopg2
import psycopg2.extras
import sys
sys.path.append('H:/')
from uth_helpers.sql_logger import db_logger, std_out_logger, pipeline_runner, create_log_table
from uth_helpers.db_utils import get_dsn, io_copy_from
from dw_ip_window_step_2 import ip_window_wrapper
log_name = 'dev.ip_dw_ip_log'

# add flags to delete certain tables?
@std_out_logger
@db_logger(log_name)
def ip_acute_drop_tables(cursor, **kwargs):
    '''drops tables'''
    cursor.execute("""
    drop table if exists dev.gm_dw_ip_window_step_1;
    drop table if exists dev.gm_dw_ip_window_step_2;
    --DROP TABLE if exists dev.gm_dw_ip_admit;
    --drop table if exists dev.gm_dw_ip_admit_claim;
    """)
    return -1

@std_out_logger
@db_logger(log_name)
def ip_acute_create_tables(cursor, **kwargs):
    '''Creates all tables in the inpatient window process'''
    cursor.execute("""
create table dev.gm_dw_ip_window_step_1 (
    data_source bpchar(4),
    uth_member_id int8,
    uth_claim_id int8,
    admit_date date,
    discharge_date date,
    from_date_of_service date,
    to_date_of_service date,
    discharge_status varchar,
    bill_type varchar,
    member_id_src text,
    claim_id_src text,
    insert_ts timestamp(0) DEFAULT LOCALTIMESTAMP
)
with (
    appendonly=true,
    orientation=column
) 
distributed by (uth_member_id);

create table dev.gm_dw_ip_window_step_2 (
    data_source bpchar(4),
    uth_member_id int8,
    uth_claim_id int8,
    admit_date date,
    discharge_date date,
    discharge_status varchar,
    bill_type varchar,
    member_id_src text,
    claim_id_src text,
    pat_group int,
    insert_ts timestamp(0) DEFAULT LOCALTIMESTAMP
)
with (
    appendonly=true,
    orientation=column
) 
distributed by (uth_member_id);

CREATE UNLOGGED TABLE if not exists dev.gm_dw_ip_admit (
    data_source bpchar(4),
    uth_member_id int8 NULL,
    enc_id int4 NULL,
    admit_date date NULL,
    discharge_date date NULL,
    enc_discharge_status varchar null,
    admit_id varchar null,
    total_charge_amount numeric,
	total_allowed_amount numeric,
	total_paid_amount numeric,
    missing_terminal_status boolean null,
    missing_terminal_status_117 boolean null,
    paid_status boolean null,
    member_id_src text null,
    insert_ts timestamp(0) DEFAULT LOCALTIMESTAMP
)
WITH (
    appendonly=true,
    orientation=column,
    compresstype=zlib
)
DISTRIBUTED BY (uth_member_id);

CREATE UNLOGGED TABLE if not exists dev.gm_dw_ip_admit_claim (
    data_source bpchar(4),
    admit_id varchar,
    uth_member_id int8 NULL,
    enc_id int4 NULL,
    admit_date date NULL,
    discharge_date date NULL,
    enc_discharge_status varchar null,
    uth_claim_id numeric,
    from_date_of_service date,
    to_date_of_service date,
    claim_type varchar null,
    charge_amount numeric(13,2),
    allowed_amount numeric(13,2),
    paid_amount numeric(13,2),
    member_id_src text null,
    claim_id_src text null,
    insert_ts timestamp(0) DEFAULT LOCALTIMESTAMP
)
WITH (
    appendonly=true,
    orientation=column,
    compresstype=zlib
)
DISTRIBUTED BY (admit_id);
""")
    return cursor.rowcount

def run_step_zero():
    acute_ip_pipeline = [ip_acute_drop_tables,
                         ip_acute_create_tables]

    sequence_description = 'IP Acute Admit: Step Zero'

    pipeline_runner(acute_ip_pipeline, sequence_description, {})         

@std_out_logger
@db_logger(log_name)
def ip_acute_insert_step_1(cursor, data_source, start_year, end_year, **kwargs):
    '''inserts acute inpatient claims from start_year to end_year'''
    cursor.execute(f"""
with ip_data as (
    select ch.data_source,
        ch.uth_member_id,
        ch.uth_claim_id,
        ch.from_date_of_service,
        case 
            when extract(year from ch.to_date_of_service) > 2050 then cd.to_date_of_service
            else ch.to_date_of_service
        end as to_date_of_service,
        admit_date,
        discharge_date,
        discharge_status,
        concat(bill_type_inst, bill_type_class, bill_type_freq) as bill_type,
        ch.member_id_src,
        ch.claim_id_src
    from data_warehouse.claim_detail cd
    join data_warehouse.claim_header ch
    on cd.uth_member_id = ch.uth_member_id
    and cd.uth_claim_id = ch.uth_claim_id
    where bill_type_inst = '1'
    and bill_type_class = '1'
    and bill_type_freq in ('1', '2', '3', '4', '7')
    and ch.year between {start_year} and {end_year}
    and ch.data_source = '{data_source}'
)
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
    bill_type,
    member_id_src,
    claim_id_src
from ip_data
group by data_source, uth_member_id, uth_claim_id, discharge_status, bill_type, member_id_src, claim_id_src;""")

    return cursor.rowcount

@std_out_logger
@db_logger(log_name)
def ip_acute_delete_step_1(cursor, start_year, end_year, **kwargs):
    '''deletes where to_date_of service is null cannot use data that doesn't have a
    end date of service'''
    cursor.execute('delete from dev.gm_dw_ip_window_step_1 where to_date_of_service is null;')
    return cursor.rowcount

@std_out_logger
@db_logger(log_name)
def ip_acute_insert_step_2(cursor, data_source, **kwargs):
    '''takes the min and max date by the claim and assigns a group id pat_group'''

    cursor.execute(f'''
insert into dev.gm_dw_ip_window_step_2 (
    data_source,
    uth_member_id,
    uth_claim_id,
    admit_date,
    discharge_date,
    discharge_status,
    bill_type,
    member_id_src,
    claim_id_src,
    pat_group
)
with pat_ids as (
	select distinct uth_member_id
	from dev.gm_dw_ip_window_step_1
	where data_source = '{data_source}'
),
pat_groups as (
	select uth_member_id, ntile(10) over (order by uth_member_id) as pat_group
	from pat_ids
),
all_inp as (
select
    data_source,
    a.uth_member_id,
    uth_claim_id,
    min(admit_date) as admit_date,
    max(discharge_date) as discharge_date,
    discharge_status,
    bill_type,
    member_id_src,
    claim_id_src,
    b.pat_group
    --ntile(10) over (order by uth_member_d) as pat_group
    --dense_rank() over (order by data_source, uth_member_id) / 1000000 as pat_group
from dev.gm_dw_ip_window_step_1 a
join pat_groups b
on a.uth_member_id = b.uth_member_id
and a.data_source = '{data_source}'
group by
    data_source,
    a.uth_member_id,
    uth_claim_id,
    bill_type,
    discharge_status,
    member_id_src,
    claim_id_src,
    pat_group
)
select
    distinct ip.data_source,
    ip.uth_member_id,
    ip.uth_claim_id,
    ac.admit_date,
    ac.discharge_date,
    ac.discharge_status,
    ac.bill_type,
    ip.member_id_src,
    ip.claim_id_src,
    ip.pat_group
from all_inp ip
join dev.gm_dw_ip_window_step_1 ac 
on ip.uth_member_id = ac.uth_member_id 
and ip.uth_claim_id = ac.uth_claim_id
and ip.data_source = ac.data_source
where ac.admit_date between ip.admit_date and ip.discharge_date;''')
    return cursor.rowcount

@std_out_logger
@db_logger(log_name)
def ip_acute_admit_tables_delete_datasource(cursor, data_source, **kwargs):
    # Deletes all rows for a data source in the temp admit tables
    cursor.execute(f'''delete from dev.gm_dw_ip_admit where data_source = '{data_source}';''')
    cursor.execute(f'''delete from dev.gm_dw_ip_admit_claim where data_source = '{data_source}';''')

def run_step_one(data_source):
    # step one; run sql
    acute_ip_pipeline = [ip_acute_insert_step_1,
                         ip_acute_delete_step_1,
                         ip_acute_insert_step_2,
                         ip_acute_admit_tables_delete_datasource]

    variable_dict = {'start_year': 2015,
                     'end_year': 2022,
                     'data_source': data_source}

    sequence_description = 'IP Acute Admit: Step One'

    pipeline_runner(acute_ip_pipeline, sequence_description, variable_dict)

@std_out_logger
@db_logger(log_name)
def ip_window_by_group(cursor, df_con, data_source, pat_group, output_table, **kwargs):

    sql_string = f'''
select * from dev.gm_dw_ip_window_step_2 
where data_source = '{data_source}' 
and pat_group = {pat_group};
                '''

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
def insert_ip_admit_claims(cursor, data_source, output_table, **kwargs):
    '''inserts all claims that occur during the ip_window timeframe'''

    cursor.execute(f'''
    insert into dev.{output_table}
    (
        data_source,
        admit_id,
        uth_member_id,
        enc_id,
        admit_date,
        discharge_date,
        enc_discharge_status ,
        uth_claim_id,
        from_date_of_service,
        to_date_of_service,
        claim_type,
        member_id_src,
        claim_id_src,
        charge_amount,
        allowed_amount,
        paid_amount
    )
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
        claim_type,
        dia.member_id_src,
        claim_id_src,
        ch.total_charge_amount,
        ch.total_allowed_amount,
        ch.total_paid_amount
    from dev.gm_dw_ip_admit dia
    inner join data_warehouse.claim_header ch 
    on dia.uth_member_id = ch.uth_member_id
    and (from_date_of_service between admit_date and discharge_date)
    and (to_date_of_service between admit_date and discharge_date)
    and dia.data_source = '{data_source}';''')

    return cursor.rowcount

@std_out_logger
@db_logger(log_name)
def dw_import(cursor, data_source, **kwargs):
    '''moves the data from dev to data warehouse; 
    also calculates 30 day readmission'''

    cursor.execute(f'''delete from data_warehouse.admission_acute_ip where data_source = '{data_source}';''')

    cursor.execute(f'''
    insert into data_warehouse.admission_acute_ip 
    (
        data_source,
        year,
        derived_uth_admission_id,
        uth_member_id,
        admit_date,
        discharge_date,
        admission_days,
        discharge_status,
        days_to_readmit,
        readmit_30,
        paid_status,
        member_id_src,
        insert_ts,
        total_charge_amount,
        total_allowed_amount,
        total_paid_amount
    )
    select
        data_source,
        extract(year from admit_date),
        admit_id,
        uth_member_id,
        admit_date,
        discharge_date,
        case 
            when discharge_date-admit_date = 0 then 1
            else discharge_date-admit_date
        end,
        enc_discharge_status,
        admit_date - lag (discharge_date) over ( partition by uth_member_id order by admit_date) as days_to_readmit,
        case 
            when admit_date - lag (discharge_date) over ( partition by uth_member_id order by admit_date)<=30 then 1
            else 0
        end as readmit_30,
        paid_status,
        member_id_src,
        insert_ts,
        total_charge_amount,
        total_allowed_amount,
        total_paid_amount
    from dev.gm_dw_ip_admit
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
        insert_ts,
        member_id_src,
        claim_id_src   
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
        insert_ts,
        member_id_src,
        claim_id_src
    from dev.gm_dw_ip_admit_claim
    where data_source = '{data_source}'
    order by uth_member_id, admit_date;''')

    return cursor.rowcount

@std_out_logger
@db_logger(log_name)
def admit_costs_update(cursor, data_source, **kwargs):
    ''' Calculate total costs for an admit episode'''

    cursor.execute(f'''
with costs as (
    select
        data_source,
        uth_member_id,
        admit_id,
        sum(charge_amount) as total_charge_amount,
        sum(allowed_amount) as total_allowed_amount,
        sum(paid_amount) as total_paid_amount
    from dev.gm_dw_ip_admit_claim ac
    where data_source = '{data_source}'
    group by 1,2,3
)
update dev.gm_dw_ip_admit as a
set
    total_charge_amount = costs.total_charge_amount,
    total_allowed_amount = costs.total_allowed_amount,
    total_paid_amount = costs.total_paid_amount
from costs
where costs.data_source = a.data_source
and costs.uth_member_id = a.uth_member_id
and costs.admit_id = a.admit_id
;''')
    return cursor.rowcount

def run_step_three(variable_dict):

    step_three_pipeline = [insert_ip_admit_claims, admit_costs_update] #,  insert_ip_admit_claims, dw_import, dw_import_claims]
    sequence_description = f'IP Acute Admit: Step Three {variable_dict["data_source"]}'
    print('running: {}'.format(sequence_description))
    pipeline_runner(step_three_pipeline, sequence_description, variable_dict)
    print('completed', end=': ')
    print(sequence_description)


if __name__ == '__main__':

    # step two: runs the python logic
    # see script dw_ip_window_step_2.py
    output_table = 'gm_dw_ip_admit'

    try:
        df_con = psycopg2.connect(get_dsn()+' keepalives=1 keepalives_idle=30 keepalives_interval=10')
        df_con.autocommit = True

        with df_con.cursor() as cursor:

            data_sources = [
                            'mdcd',
                            'mcpp', 
                            'mhtw', 
                            'mcrt',
                            'mcrn',
                            'optz',
                            'optd',
                            'truc', 
                            'trum',
                            ]

            # clears tables from step 1
            run_step_zero()

            for data_source in data_sources:
                print(data_source)

                # inserts all inpatient claims; adds a group identifier
                run_step_one(data_source)

                cursor.execute(f'''select distinct data_source, pat_group
                            from dev.gm_dw_ip_window_step_2
                            where data_source in ('{data_source}')
                            order by data_source, pat_group
                            ;''')
                results = cursor.fetchall()
                print(results)

                # iterates through the groups to determine ip window
                # allows for processing in memory
                for pat_group in results:
                    variable_dict = {'df_con': df_con, 'data_source': pat_group[0],
                                    'pat_group': pat_group[1], 
                                    'output_table': output_table}
                    run_step_two(variable_dict)
        
                # insert all claims within ip window and move to DW
                variable_dict = {'df_con': df_con, 'data_source': data_source, 
                                'output_table': output_table+'_claim'}
                run_step_three(variable_dict)
    except:
        raise
    finally:
        df_con.close()

    