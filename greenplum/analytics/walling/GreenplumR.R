library(GreenplumR)

conn.id <- db.connect(conn.pkg = "RPostgreSQL",
                      dbname = 'uthealth', 
                      host = 'greenplum01.corral.tacc.utexas.edu',
                      port = 5432,
                      user = 'walling',
                      password = 'mUniop91*')
