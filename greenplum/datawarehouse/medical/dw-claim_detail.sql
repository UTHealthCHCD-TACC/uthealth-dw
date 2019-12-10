

drop table dev.claim_detail_v1;

create table dev.claim_detail_v1 (  
		data_source char(4),
		uth_claim_id numeric, 
		claim_sequence_number int4,
		uth_member_id bigint, 
		from_date_of_service date,
		to_date_of_service date,
		month_year_id int4, 
		perf_provider_id int,
		bill_provider_id int,
		ref_provider_id int,
		place_of_service int, 
		network_ind bool,
		network_paid_ind bool,
		admit_date date,
		discharge_date date,
		procedure_cd text,
		procedure_type text,
		proc_mod_1 char(1), 
		proc_mod_2 char(1), 
		revenue_cd char (4),
		charge_amount numeric(13,2),
		allowed_amount numeric(13,2),
		paid_amount numeric(13,2),
		copay numeric(13,2),
		deductible numeric(13,2),
		coins numeric(13,2),
		cob numeric(13,2),	
		bill_type_inst char(1),
		bill_type_class char(1),
		bill_type_freq char(1),
		units int4,
		drg_cd text,  --drg vs mdc 
		drg_type text
) with (appendonly=true, orientation = column)
distributed by (uth_claim_id);


analyze dev.claim_detail_v1;

-----------------------------------------------------------------------------------------------

drop table dev.claim_header_v1;

create table dev.claim_header_v1 (
		data_source char(4),
		uth_claim_id numeric, 
		uth_member_id bigint, 
		claim_type text,
		place_of_service text,
		admission_id text,
		total_charge_amount numeric(13,2),
		total_allowed_amount numeric(13,2),
		total_paid_amount numeric(13,2)
) with (appendonly=true, orientation = column)
distributed by (uth_claim_id);


analyze dev.claim_header_v1;



select dbo.set_all_perms();

-----------------------------------------------------------------------------------------------


select pay, netpay, ntwkprov, ntwkprov::bool 
from truven.ccaeo; 



insert into dev.claim_detail_v1 (  data_source, uth_claim_id, claim_sequence_number,  uth_member_id, from_date_of_service, to_date_of_service,
								   month_year_id, perf_provider_id, place_of_service, network_ind, network_paid_ind,
								   procedure_cd, procedure_type, proc_mod_1, proc_mod_2, revenue_code,
								   allowed_amount, paid_amount, deductible, copay, coins, cob, units,
								   bill_type_inst, bill_type_class, bill_type_freq)		
								   
								   
select 'trvc', c.uth_claim_id, a.seqnum, c.uth_member_id, a.svcdate, a.tsvcdat,
       b.month_year_id, a.provid, a.stdplac, a.ntwkprov::bool, a.paidntwk::bool, 
       a.proc1, a.proctyp, substring(a.procmod,1,1), substring(procmod,2,1), revcode, 
       a.pay, a.netpay, a.deduct, a.copay, a.coins, a.cob, trunc(a.qty,0), 
       substring(d.billtyp,1,1), substring(d.billtyp,2,1), substring(d.billtyp,3,1) 
     --  d.caseid, d.fachdid, d.billtyp, facprof
     --  dx1, dx2, dx3, dx4, dxver,
from truven.ccaeo a 
  join reference_tables.ref_month_year b 
    on month_int = extract(month from svcdate) 
   and year_int = year
  join data_warehouse.dim_uth_claim_id c 
    on c.data_source = 'trvc'
   and c.data_year = trunc(a.year,0)
   and c.claim_id_src = a.msclmid::text
   and c.member_id_src = a.enrolid::text
  left outer join truven.ccaef d 
    on d.msclmid = a.msclmid 
   and d.year = a.year 
   and d.enrolid = a.enrolid 
where a.msclmid is not null
  and a.year between 2015 and 2017
  ;

 
 select count(distinct msclmid ) from truven.ccaes;


 
 select * from dev.claim_detail_v1 where uth_claim_id = 152255296912;
 
 
select caseid, msclmid, year, enrolid, *
from truven.ccaes 
where caseid = 852789 and year = 2011;
 

select count(*) from truven.ccaef_wc;


drop function claim_detail_build();

create or replace function claim_detail_build () returns void
as $FUNC$ 
declare
	r_data_source text; 
	r_uth_claim_id numeric; 
	r_claim_id_src int; 
	r_claim_seq int; 
	r_enrolid bigint; 
	r_uth_member_id bigint;
	r_year int; 
	r_from_date_of_service date; 
	r_to_date_of_service date; 
	r_month_year_id int; 
	r_perf_provider_id int; 
	r_bill_provider_id int;
	r_ref_provider_id int; 
	r_network_ind char(1); 
	r_network_paid_ind char(1); 
	r_procedure_code text; 
	r_procedure_type text; 
	r_procmod char(2);
	r_revenue_cd char(4); 
	r_charge_amount numeric; 
	r_allowed_amount numeric; 
	r_paid_amount numeric; 
	r_copay numeric; 
	r_deductible numeric; 
	r_coins numeric; 
	r_cob numeric;	
	r_bill_type char(3); 
	r_units int4; 
	r_drg_cd text; 
	r_drg_type char(4);
	r_dx1 text; 
	r_dx2 text; 
	r_dx3 text; 
	r_dx4 text; 
	r_dxver text; 
---
	r_fachdid numeric;
	r_facprof char(1);
	r_place_of_service numeric;
begin
	for r_data_source, r_claim_id_src, r_claim_seq, r_year, r_from_date_of_service, r_to_date_of_service, r_enrolid,
		r_allowed_amount, r_paid_amount, r_deductible, r_copay, r_coins, r_cob,
		r_procedure_code, r_procedure_type, r_procmod, r_revenue_cd,
		r_perf_provider_id, r_place_of_service, r_network_ind, r_network_paid_ind,
		r_units, r_fachdid, r_facprof,
		r_dx1, r_dx2, r_dx3, r_dx4, r_dxver
	 in 
		select 'trvx', msclmid, seqnum, "year", svcdate, tsvcdat, enrolid,
		       netpay, pay, deduct, copay, coins, cob,
		       proc1, proctyp, procmod, revcode, 
		       provid, stdplac, ntwkprov, paidntwk,
		       trunc(qty,0), fachdid, facprof, 
		       dx1, dx2, dx3, dx4, dxver
		from truven.ccaeo_wc a 
		where msclmid is not null
		  and year between 2015 and 2017
		  limit 25
		
	loop 	
		--get month_year_id and uth_member_id, assign uth_claim_id
		select month_year_id into r_month_year_id from reference_tables.ref_month_year where month_int = extract(month from r_from_date_of_service) and year_int = r_year;
	
		select uth_member_id into r_uth_member_id from data_warehouse.dim_uth_member_id where data_source = 'trvc' and trunc(member_id_src::numeric,0) = r_enrolid;
	
		r_uth_claim_id := ( substring(r_year::text,3,2) || substring(r_uth_member_id::text,1,2) ||  substring(r_uth_member_id::text,9,3) || right(r_claim_id_src::text,4)  )::numeric;
		
		--check facility header
		if r_fachdid is not null then
			select substring(billtyp,1,3) into r_bill_type from truven.ccaef_wc a where a.fachdid = r_fachdid;
		end if;
	
		--check for claim header entry
		perform 1 from dev.claim_header_v1 where uth_claim_id = r_uth_claim_id;
		if not found then
			insert into dev.claim_header_v1 ( data_source, uth_claim_id, uth_member_id, admit_id, claim_type )
				   values (r_data_source, r_uth_claim_id, r_uth_member_id, r_fachdid, r_facprof);
		end if;
	
		--claim detail
		insert into dev.claim_detail_v2 (data_source, uth_claim_id, claim_id_src, claim_sequence_number, 
										 uth_member_id, from_date_of_service, to_date_of_service, month_year_id, 
										 perf_provider_id, network_ind, network_paid_ind, 
										 procedure_cd, procedure_type, proc_mod_1, proc_mod_2, 
										 revenue_code, allowed_amount, paid_amount, 
										 deductible, copay, coins, cob, units,
										 bill_type_inst, bill_type_class, bill_type_freq
										 ) 
		                         values (r_data_source, r_uth_claim_id, r_claim_id_src, r_claim_seq, 
		                         		 r_uth_member_id, r_from_date_of_service, r_to_date_of_service, r_month_year_id, 
		                        		 r_perf_provider_id, r_network_ind::bool, r_network_paid_ind::bool,  
		                         		 r_procedure_code, r_procedure_type, substring(r_procmod,1,1), substring(r_procmod,2,1), 
		                        		 r_revenue_cd, r_allowed_amount, r_paid_amount, 
		                        		 r_deductible, r_copay, r_coins, r_cob, r_units,
		                        		 substring(r_bill_type,1,1), substring(r_bill_type,2,1), substring(r_bill_type,3,1)
		                        		 );
		
	end loop;	
end $FUNC$
language 'plpgsql';
----------------------------------------------------------------------------------



select claim_detail_build();


select *, 	
		row_number() over (
		partition by uth_claim_id
		order by claim_sequence_number) rownum 
from dev.claim_detail_v2 
where data_source = 'trvx' 
order by uth_claim_id, claim_sequence_number; 


select * from dev.claim_header_v1
order by uth_claim_id;



delete from dev.claim_detail_v2 where data_source = 'trvx';

delete from dev.claim_header_v1;
		