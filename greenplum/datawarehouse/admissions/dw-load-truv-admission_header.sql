
create table dev.wc_mdcrf with (appendonly=true, orientation=column)
as select * from truven.mdcrf distributed by (enrolid)
;

vacuum analyze dev.wc_mdcrf;

insert into dw_qa.admission_header (data_source, year, uth_admission_id , uth_member_id,
                                    admit_date, discharge_date, total_paid_amount,
                                    admission_id_src, member_id_src , table_id_src )
select 'truv', extract(year from min(a.svcdate)), b.uth_admission_id, b.uth_member_id,  
       min(a.svcdate), max(a.tsvcdat), sum(a.netpay) as paid, 
         a.caseid::text, a.enrolid::text, 'mdcrf'
from dev.wc_mdcrf a
  join data_warehouse.dim_uth_admission_id b 
    on a.enrolid::text = b.member_id_src 
   and a.caseid::text = b.admission_id_src 
group by b.uth_admission_id, b.uth_member_id, a.caseid::text, a.enrolid::text
;

drop table dev.wc_mdcrf;



create table dev.wc_mdcri with (appendonly=true, orientation=column)
as select * from truven.mdcri distributed by (enrolid)
;

vacuum analyze dev.wc_mdcri;

insert into dw_qa.admission_header (data_source, year, uth_admission_id , uth_member_id,
                                    admit_date, discharge_date, total_allowed_amount, total_paid_amount,
                                    admission_id_src, member_id_src , table_id_src )
select 'truv', extract(year from min(a.admdate)), b.uth_admission_id, b.uth_member_id,  
       min(a.admdate), max(a.disdate), sum(totnet) as alw, sum(a.totpay) as paid, 
         a.caseid::text, a.enrolid::text, 'mdcri'
from dev.wc_mdcri a
  join data_warehouse.dim_uth_admission_id b 
    on a.enrolid::text = b.member_id_src 
   and a.caseid::text = b.admission_id_src 
group by b.uth_admission_id, b.uth_member_id, a.caseid::text, a.enrolid::text
;

drop table dev.wc_mdcri;


------commercial

create table dev.wc_ccaef with (appendonly=true, orientation=column)
as select * from truven.ccaef distributed by (enrolid)
;

vacuum analyze dev.wc_ccaef;

insert into dw_qa.admission_header (data_source, year, uth_admission_id , uth_member_id,
                                    admit_date, discharge_date, total_paid_amount,
                                    admission_id_src, member_id_src , table_id_src )
select 'truv', extract(year from min(a.svcdate)), b.uth_admission_id, b.uth_member_id,  
       min(a.svcdate), max(a.tsvcdat), sum(a.netpay) as paid, 
         a.caseid::text, a.enrolid::text, 'ccaef'
from dev.wc_ccaef a
  join data_warehouse.dim_uth_admission_id b 
    on a.enrolid::text = b.member_id_src 
   and a.caseid::text = b.admission_id_src 
group by b.uth_admission_id, b.uth_member_id, a.caseid::text, a.enrolid::text
;

drop table dev.wc_ccaef;



create table dev.wc_ccaei with (appendonly=true, orientation=column)
as select * from truven.ccaei distributed by (enrolid)
;

vacuum analyze dev.wc_ccaei;

insert into dw_qa.admission_header (data_source, year, uth_admission_id , uth_member_id,
                                    admit_date, discharge_date, total_allowed_amount, total_paid_amount,
                                    admission_id_src, member_id_src , table_id_src )
select 'truv', extract(year from min(a.admdate)), b.uth_admission_id, b.uth_member_id,  
       min(a.admdate), max(a.disdate), sum(totnet) as alw, sum(a.totpay) as paid, 
         a.caseid::text, a.enrolid::text, 'ccaei'
from dev.wc_ccaei a
  join data_warehouse.dim_uth_admission_id b 
    on a.enrolid::text = b.member_id_src 
   and a.caseid::text = b.admission_id_src 
group by b.uth_admission_id, b.uth_member_id, a.caseid::text, a.enrolid::text
;

drop table dev.wc_ccaei;






