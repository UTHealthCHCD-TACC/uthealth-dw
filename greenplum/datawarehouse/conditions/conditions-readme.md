## Conditions

The latest codeset is stored on SQL Server in `CND.dbo.codeset` database. 

The code for conditions on Greenplum, `1-create-codeset...` drops the conditions table and then says to re-import the conditions list. If you are running the code in DBeaver, you can either import using DBeaver - from DB to DB, or you can export the SQL Server table to .csv and impost using `psql`. 

