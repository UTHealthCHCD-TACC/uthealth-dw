# install.packages('RPostgres')
library(DBI)

# Connect to a specific postgres database
con <- dbConnect(RPostgres::Postgres(),
                 dbname = 'uthealth',
                 sslmode='require', 
                 host = 'greenplum01.corral.tacc.utexas.edu', # i.e. 'ec2-54-83-201-96.compute-1.amazonaws.com'
                 port = 5432, # or any other port specified by your DBA
                 user = 'walling',
                 password = '<password>') # DON'T COMMIT PW TO GIT!!!!!

dbListTables(con)

time <- Sys.time()
data <- dbGetQuery(con, "SELECT * FROM dev.dw_recursive_test order by patid, eligeff, eligend")
Sys.time() - time