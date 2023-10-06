import psycopg2
import sys
sys.path.append('H:/uth_helpers')
from db_utils import get_dsn

connection = psycopg2.connect(get_dsn()+' keepalives=1 keepalives_idle=30 keepalives_interval=10')
connection.autocommit = True

with connection.cursor() as cursor:

    print('backup old counts just in case')

    query = '''
drop table if exists qa_reporting.truven_counts_old;

SELECT *
INTO qa_reporting.truven_counts_old
from qa_reporting.truven_counts;

drop table if exists qa_reporting.truven_counts;
create table qa_reporting.truven_counts
(year int,
table_name text,
row_count bigint,
pat_count bigint,
clm_count bigint,
reported_row_count bigint,
row_count_difference bigint,
row_percent_difference float,
last_updated date,
db_version int,
report_version numeric
);
    '''

    cursor.execute(query)

    query = '''
    grant select on qa_reporting.truven_counts_old to uthealth_analyst;
    grant select on qa_reporting.truven_counts to uthealth_analyst;
    grant insert on qa_reporting.truven_counts to uthealth_dev;
    grant update on qa_reporting.truven_counts to uthealth_dev;
    grant delete on qa_reporting.truven_counts to uthealth_dev;
    '''

    cursor.execute(query)

    print('A - enrollment tables')

    query = '''
insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'ccaea', count(*), count(distinct enrolid), 0
  from truven.ccaea
group by year;
    '''

    cursor.execute(query)

    query = '''
insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'mdcra', count(*), count(distinct enrolid), 0
  from truven.mdcra
group by year;
    '''

    cursor.execute(query)

    print('F - facility header tables')

    query = '''
insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'ccaef', count(*), count(distinct enrolid), count(distinct claim_id_derv)
  from truven.ccaef
group by year;
    '''

    cursor.execute(query)

    query = '''
insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'mdcrf', count(*), count(distinct enrolid), count(distinct claim_id_derv)
  from truven.mdcrf
group by year;
    '''

    cursor.execute(query)

    print('I - inpatient admission tables')

    query = '''
-- possibly do enrolid::text || admdate::text for clmid
insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'ccaei', count(*), count(distinct enrolid), count(distinct enrolid::text || admdate::text)
  from truven.ccaei
group by year;
    '''

    cursor.execute(query)

    query = '''
insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'mdcri', count(*), count(distinct enrolid), count(distinct enrolid::text || admdate::text)
  from truven.mdcri
group by year;
    '''

    cursor.execute(query)

    print('O - outpatient tables')

    query = '''
insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'ccaeo', count(*), count(distinct enrolid), count(distinct claim_id_derv)
  from truven.ccaeo
group by year;
    '''

    cursor.execute(query)

    query = '''
insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'mdcro', count(*), count(distinct enrolid), count(distinct claim_id_derv)
  from truven.mdcro
group by year;
    '''

    cursor.execute(query)

    print('S - Inpatient Services table')

    query = '''
insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'ccaes', count(*), count(distinct enrolid), count(distinct claim_id_derv)
  from truven.ccaes
group by year;
    '''

    cursor.execute(query)

    query = '''
insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'mdcrs', count(*), count(distinct enrolid), count(distinct claim_id_derv)
  from truven.mdcrs
group by year;
    '''

    cursor.execute(query)

    print('T - Detailed Enrollment table')

    query = '''
insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'ccaet', count(*), count(distinct enrolid), 0
  from truven.ccaet
group by year;
    '''

    cursor.execute(query)

    query = '''
insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'mdcrt', count(*), count(distinct enrolid), 0
  from truven.mdcrt
group by year;
    '''

    cursor.execute(query)

    print('D - RX')

    query = '''
insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'ccaed', count(*), count(distinct enrolid), count(distinct enrolid::text || ndcnum::text || svcdate::text)
  from truven.ccaed
group by year;
    '''

    cursor.execute(query)

    query = '''
insert into qa_reporting.truven_counts
(year, table_name, row_count, pat_count, clm_count)
select year, 'mdcrd', count(*), count(distinct enrolid), count(distinct enrolid::text || ndcnum::text || svcdate::text)
  from truven.mdcrd
group by year;
    '''

    cursor.execute(query)

    print('P ?')

    query = '''
insert into qa_reporting.truven_counts
(year, table_name, row_count)
select year, 'ccaep', count(*)
  from truven.ccaep
group by year;
    '''

    cursor.execute(query)

    query = '''
insert into qa_reporting.truven_counts
(year, table_name, row_count)
select year, 'mdcrp', count(*)
  from truven.mdcrp
group by year;
    '''

    cursor.execute(query)

    print('Getting Data Version')

    cursor.execute('select distinct table_name from qa_reporting.truven_counts;')

    table_names = cursor.fetchall()

    for table in table_names:
        query = f'''update qa_reporting.truven_counts b
        set db_version = a.version
        from (
            select distinct '{table[0]}' as table_name, year, version 
            from truven.{table[0]}
        ) a
        where b.table_name = a.table_name
        and a.year = b.year
        ;
        '''

        cursor.execute(query)