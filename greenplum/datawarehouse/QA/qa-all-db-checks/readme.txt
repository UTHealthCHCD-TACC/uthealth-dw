-----------QA All DB Checks-----------

The purpose of this check is to know that all the data that is exported from sql server > csv > greenplum has the amount of data
Steps are:

--- check counts in SQL Server
		use pyodbc
			- example of code is:
					greenplum\datawarehouse\QA\qa-all-db-checks\script-examples\pyodbc-sql-server-connection.ipynb

--- check csvs when exporting
	  not sure about this one will have to do some googling - reading into memory is the issue, once on linux can just use wc -l

		jeff has perl script but idk anything about perl greenplum\datawarehouse\QA\qa-all-db-checks\script-examples\readline_records.pl

--- check counts in greenplum
	  you can use pyodbc or psycopg
		---- example of code is in:
				greenplum\datawarehouse\QA\qa-all-db-checks\script-examples\psycopg-connection (this one does a lot of sql hard coded in python)
				greenplum\datawarehouse\QA\qa-all-db-checks\script-examples\psycopg-run-external-sql.py (this one runs outside sql scripts)
				misc others in greenplum\datawarehouse\QA\qa-all-db-checks\script-examples\

--- combine in some table or report
		---- qa_reporting schema may be best place for now, madhuri had talked about making a dashboard for it, but not sure who has time for that right now but we should have something like that
		---- may be easiest to use pandas at this point

Easiest way to use python is to use VSCode:
Build in notebook
Transfer to Script

May be best to set up your own conda env

Let's do count(*) for now
