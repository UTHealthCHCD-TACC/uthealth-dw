

import psycopg2
import psycopg2.extras
import getpass 
import datetime
import time 

# Greenplum connection details
host = 'greenplum01.corral.tacc.utexas.edu'
port = 5432
database = 'uthealth'
username = 'wcough'

# user's username and password info (given by user input)
#username = input('Username:')
# getpass hides user input 

print('Enter password')
password = getpass.getpass()

connection = psycopg2.connect(database=database,
                             host=host,
                             user=username, 
                             port=port, 
                             password=password)


print('connected to database')

work_path = 'H:\\uthealth-dw\\greenplum\\datawarehouse\\Data_Refresh\\Optum\\'




files = [ work_path + 'optd-load-claim_header.sql',
          work_path + 'optz-load-claim_header.sql']

completed_scripts = []
noncompleted_scripts = []
bad_queries = []

for file in files:
    try:
        file_contents = open(file, 'r').read()
        queries = file_contents.split(';')[:-1]
        
        print('executing sql script '+file)
        with connection.cursor() as cursor:
            for query in queries:
                start_time = datetime.datetime.now()
                print(start_time)
                print(query[:50])
                try:
                    cursor.execute(query)
                except Exception as e:
                    # move to next file if exception occurs
                    noncompleted_scripts.append(file)
                    bad_queries.append(query)
                    raise Exception(e)
                else:
                    connection.commit()
                    time_executed = time.time() - start_time.timestamp()
                    print(int(time_executed/3600), 'hours', 
                        int(time_executed/60), 'minutes',
                        int(time_executed%60), 'seconds', 
                        time_executed%1, 'milliseconds')
    except Exception as e:
        print(e)
    else:
        print('finished executing script '+file)
        completed_scripts.append(file)

    print(50*'-')

print('Completed executing the following scripts: ')
print(completed_scripts)
if noncompleted_scripts:
    print('Issues executing the following scripts: ')
    print(noncompleted_scripts)
    for i in bad_queries:
        print(i)
connection.close()