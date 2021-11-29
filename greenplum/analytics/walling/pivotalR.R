library(PivotalR)

conn.id <- db.connect(conn.pkg = "RPostgreSQL",
                       dbname = 'dbname=uthealth', 
                       host = 'greenplum01.corral.tacc.utexas.edu',
                       port = 5432,
                       user = 'walling',
                       password = 'mUniop91*')

install.packages('RPostgreSQL')
install.packages('PivotalR')

base::list(sslmode="require", connect_timeout="10")

drv <- dbDriver( "PostgreSQL" )
db <- 'myDatabase'  
host_db <- 'mydb1.example.com'  
db_port <- '98939'  
db_user <- 'henryviii'  
db_password <- 'happydays'

conn <- dbConnect(drv, dbname=db, host=host_db, port=db_port, user=db_user, password=db_password)

con <- dbConnect(RPostgres::Postgres(),
                 dbname = 'uthealth', 
                 host = 'greenplum01.corral.tacc.utexas.edu', # i.e. 'ec2-54-83-201-96.compute-1.amazonaws.com'
                 port = 5432, # or any other port specified by your DBA
                 user = 'walling',
                 password = 'mUniop91*') # DON'T COMMIT PW TO GIT!!!!!

x <- db.data.frame("tableau.enrollment_yearly_optz_truv")
