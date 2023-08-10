
psql -h greenplum01.corral.tacc.utexas.edu -d uthealth -U xrzhang -W -f "H:/GitHub/UTH-CHCD/uthealth-dw/greenplum/datawarehouse/staging-load/medicaid/01-setup/02-dw-medicaid-load-dim_uth_claim_id.sql"

psql -h greenplum01.corral.tacc.utexas.edu -d uthealth -U xrzhang -W -f "H:/GitHub/UTH-CHCD/uthealth-dw/greenplum/datawarehouse/staging-load/medicaid/01-setup/03-dw-medicaid-dim_uth_rx_claim_id.sql"




psql -h greenplum01.corral.tacc.utexas.edu -d uthealth -U xrzhang -W -f "H:/GitHub/UTH-CHCD/uthealth-dw/greenplum/datawarehouse/staging-load/medicaid/psql/02-dw-medicaid-member_enrollment_monthly-load.sql"









