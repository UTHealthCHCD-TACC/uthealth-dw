import io
import os
from dotenv import dotenv_values
import keyring


def get_dsn(dot_env_file='xz_dw.env'):
    '''reads a .env file that contains the credentials to
    connect to the uthealth database.
    The .env file must be in the same directory as db_utils and contain
        the dbname, user, port, and host
    A keyring password must be made for the dbname and user:
        keyring.set_password('dbname', 'username', 'password')

    '''

    # gets env file location and loads env values in to a dictionary
    dot_env_path = os.path.join(os.path.dirname(__file__), dot_env_file)
    config = dotenv_values(dot_env_path)

    # config must have the following keys
    assert set(list(config.keys())) == {'dbname', 'user', 'port', 'host'}

    dsn_list = []
    for key, value in config.items():
        dsn_list.append(key+'='+value)

    dsn_list.append("password=" + keyring.get_password(config['dbname'],
                                                       config['user']))
    return " ".join(dsn_list)


def io_copy_from(con, df, schema, table):
    '''
    con: psycopg2 connection
    df: pd.dataframe
    schema: name of schema
    table: string of table to go into

    Copys pandas dataframe to database
    '''

    f = io.StringIO()
    df.to_csv(f, index=False, header=False, sep='\t')
    f.seek(0)
    with con.cursor() as cursor:
        # bug in psycopg2 that you can't put table as schema.tables
        # have to set schema then import to table
        cursor.execute(f'set search_path to {schema}, public')
        cursor.copy_from(f, table, sep='\t')
        rowcount = cursor.rowcount
    return rowcount


def sql_script_reader(file_name):
    # Open and read the file
    with open(file_name, 'r') as f:
        sql_file = f.read()
    sql_command_list = sql_file.split(';\n')
    sql_command_list = [i for i in sql_command_list if len(i.strip('\n')) > 0]
    return sql_command_list


def prepare_sql_command_sequence(sql_command_list, sequence_description):

    sql_command_sequence = []
    for count, sql_command in enumerate(sql_command_list):
        sql_dict = {}
        sql_dict['sequence_description'] = sequence_description

        sql_dict['command_sequence'] = count
        sql_dict['command_description'] = ''
        sql_dict['sql_command'] = ''

        initial_comment = True

        for line in sql_command.split('\n'):
            if line.startswith('--'):
                pass
            else:
                initial_comment = False
            if initial_comment:
                sql_dict['command_description'] += line[2:]
            else:
                sql_dict['sql_command'] += line

        sql_dict['sql_command'] = sql_dict['sql_command'].strip()
        sql_dict['command_description'] = sql_dict['command_description'].strip()

        sql_dict['status'] = 'in progress'
        sql_command_sequence.append(sql_dict)

    return sql_command_sequence
