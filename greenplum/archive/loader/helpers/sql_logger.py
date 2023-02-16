import inspect
import logging
import psycopg2
from chcd_py.helpers.db_utils import get_dsn
from functools import wraps
from datetime import datetime


def std_out_logger(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            logging.basicConfig(format='%(asctime)s: %(message)s',
                                level=logging.INFO)
            logging.info(f"function {func.__name__} called")
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            logging.exception(f"Exception raised in {func.__name__}. exception: {str(e)}")
            raise e
    return wrapper


def create_log_table(cursor, schema, log_table_name):
    '''makes a log table'''
    try:
        cursor.execute(f'''
        CREATE TABLE {schema}.{log_table_name}(
            id serial primary key,
            sequence_description varchar(50) NULL,
            command_sequence int4 NULL,
            command_text text NULL,
            command_description text null,
            command_start_time timestamp default NOW(),
            command_end_time timestamp NULL,
            row_count int8 NULL,
            status varchar(20) NULL,
            error_code text NULL,
            userid text NULL DEFAULT "current_user"()
        )
        DISTRIBUTED BY (id);

        create trigger {log_table_name}_trigger
        before update on {schema}.{log_table_name}
        for each row execute procedure dev.trigger_update_ts();''')
    except psycopg2.errors.DuplicateTable:
        print(f'{schema}.{log_table} already exists')
        pass
    except:
        raise
    return -1


def db_log_helper(cursor, log_table, sql_log_dict):

    if sql_log_dict['status'] == 'in progress':
        cursor.execute(f'''insert into {log_table}(sequence_description,
        command_sequence, command_text, command_description, status)
        values (%(sequence_description)s, %(command_sequence)s,
        %(sql_command)s, %(command_description)s,
        %(status)s)''', sql_log_dict)
        cursor.execute(f'select max(id) as id from {log_table}')
        log_id = cursor.fetchone()[0]
        return log_id

    elif sql_log_dict['status'] == 'completed':
        cursor.execute(f'''update {log_table} set status = %(status)s,
        row_count = %(row_count)s, command_text = %(command_text)s
        where id = %(log_id)s''', sql_log_dict)

    elif sql_log_dict['status'] == 'failed':
        cursor.execute(f'''update {log_table} set status = %(status)s,
        error_code = %(error_code)s
        where id = %(log_id)s''', sql_log_dict)
    return sql_log_dict['log_id']


def db_logger(log_table):

    def decorate(func):
        @wraps(func)
        def wrapper(*args, **kwargs):

            # if a dict then it takes the dictionary of the query meta data
            # else takes the kwargs that store the data...
            try:
                if type(args[1]) == dict:
                    sql_log_dict = args[1]
            except IndexError:
                sql_log_dict = kwargs

                doc_str = inspect.getdoc(func)
                if doc_str is not None:
                    sql_log_dict['command_description'] = func.__name__ + ': ' + doc_str
                else:
                    sql_log_dict['command_description'] = func.__name__

                sql_log_dict['sql_command'] = inspect.getsource(func)
                pass

            sql_log_dict['status'] = 'in progress'

            try:
                log_con = psycopg2.connect(get_dsn())
                log_con.autocommit = True
                # logs the initialization
                with log_con.cursor() as log_cursor:
                    log_id = db_log_helper(log_cursor, log_table, sql_log_dict)
                sql_log_dict['log_id'] = log_id
                # result should be row count or list of results
                result = func(*args, **kwargs)

                if args[0].query is None:
                    sql_log_dict['command_text'] = sql_log_dict['sql_command']
                else:
                    sql_log_dict['command_text'] = args[0].query.decode("utf-8")

                if type(result) == int:
                    sql_log_dict['row_count'] = result
                elif type(result) == list:
                    sql_log_dict['row_count'] = len(result)
                elif type(result) == tuple:
                    sql_log_dict['row_count'] = result[0]
                else:
                    sql_log_dict['row_count'] = -1

                # if successful logs the completion
                sql_log_dict['status'] = 'completed'
                with log_con.cursor() as log_cursor:
                    db_log_helper(log_cursor, log_table, sql_log_dict)
                return result
            except Exception as e:
                # if failed logs the failure and raises error
                sql_log_dict['status'] = 'failed'
                sql_log_dict['error_code'] = str(e)
                with log_con.cursor() as log_cursor:
                    db_log_helper(log_cursor, log_table, sql_log_dict)
                raise

            finally:
                log_con.close()

        return wrapper
    return decorate


log_table = 'dev.gm_log_test'
@std_out_logger
@db_logger(log_table)
def logged_query_runner(query_cursor, query_dict):
    try:
        query_cursor.execute(query_dict['sql_command'], query_dict['params'])
    except KeyError:
        query_cursor.execute(query_dict['sql_command'])
    row_count = query_cursor.rowcount
    return row_count


def pipeline_runner(pipeline_list, sequence_description, arg_dict, autocommit=False):
    assert type(pipeline_list) == list
    assert type(arg_dict) == dict
    arg_dict['command_sequence'] = 1
    arg_dict['sequence_description'] = sequence_description

    pipeline_start_time = datetime.now()
    arg_dict['sequence_start_time'] = f'{pipeline_start_time:%Y-%m-%d %H:%M:%S}'

    try:
        query_con = psycopg2.connect(get_dsn())
        query_con.autocommit = autocommit
        with query_con:
            for pipeline_sequence in pipeline_list:
                with query_con.cursor() as cursor:
                    if callable(pipeline_sequence):
                        pipeline_sequence(cursor, **arg_dict)
                    elif type(pipeline_sequence) == dict:
                        pipeline_sequence['command_sequence'] = arg_dict['command_sequence']
                        pipeline_sequence['sequence_description'] = sequence_description

                        logged_query_runner(cursor, pipeline_sequence)
                    arg_dict['command_sequence'] += 1
                if autocommit:
                    query_con.commit()
    except:
        if query_con.autocommit is False:
            print('Transaction rolled back!')
        raise
    finally:
        query_con.close()


if __name__ == '__main__':
    log_table = 'dev.gm_log_test'
    sql_sequence = [{'sql_command': 'select * from data_warehouse.admission limit %s',
                     'command_description': 'final test',
                     'params': (40,)},
                    {'sql_command': 'select * from data_warehouse.admission limit %s',
                     'command_description': 'final test',
                     'params': (20,)}]

    pipeline_runner(sql_sequence, sequence_description='pipeline_test', arg_dict={}, autocommit=True)