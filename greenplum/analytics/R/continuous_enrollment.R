library(DBI)
# Connect to a specific postgres database i.e. Heroku
con <- dbConnect(RPostgres::Postgres(),dbname = 'uthealth', 
                 host = 'greenplum01.corral.tacc.utexas.edu', # i.e. 'ec2-54-83-201-96.compute-1.amazonaws.com'
                 port = 5432, # or any other port specified by your DBA
                 user = 'walling',
                 password = '<password>')

dbListTables(con)

time <- Sys.time()
data_all <- dbGetQuery(con, "SELECT * FROM dev.dw_recursive_test order by patid, eligeff, eligend")
Sys.time() - time

data = data_all[data_all$patid==33006126418,]

library(dplyr)

time <- Sys.time()
consecutive <- data %>%
  #mutate_at(vars(eligend, eligeff), funs(as.Date)) %>%
  group_by(patid) %>%
  slice(which.max(eligeff > ( lead(eligend) + 1 ) | is.na(eligeff > ( lead(eligend) + 1 ))))
Sys.time() - time



# Dummy Data

id = 1

start = as.Date(c('2011-06-01','2014-08-01','2017-10-01','2017-11-01'))

end = as.Date(c('2011-06-30','2017-09-30','2017-10-31', '2017-12-31'))

data <- data.frame(id=rep(id,4), start, end)

data %>% 
  group_by(id) %>% 
  mutate(group = ifelse(start - lag(end) == 1, lag(start), start))


