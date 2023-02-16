drop table if exists dev.am_scd_members;

select distinct ch.data_year , ch.member_id_src 
	into dev.am_scd_members
	from data_warehouse.claim_diag cd , data_warehouse.claim_header ch , data_warehouse.member_enrollment_yearly mey 	
	where cd.uth_member_id = ch.uth_member_id 
		and cd.uth_claim_id = ch.uth_claim_id 
		and cd.uth_member_id = mey.uth_member_id 
		and cd.data_source = 'truv'
		and cd.data_year in (2011,2012,2013,2014,2015)		
		and cd.diag_cd in ('28241','28242','2826','28260','28261','28262','28263','28264','28265',
						   '28266','28267','28268','28269')
		and mey.rx_coverage = 1;
	
insert into dev.am_scd_members
	select distinct ch.data_year , ch.member_id_src 		
		from data_warehouse.claim_diag cd , data_warehouse.claim_header ch , data_warehouse.member_enrollment_yearly mey 		
		where cd.uth_member_id = ch.uth_member_id 
			and cd.uth_claim_id = ch.uth_claim_id 
			and cd.uth_member_id = mey.uth_member_id 
			and cd.data_source = 'truv'
			and cd.data_year in (2016,2017,2018,2019)			
			and cd.diag_cd in ('D57','D570','D5700','D5701','D5702','D571','D572','D5720','D5721','D57211',
						       'D57212','D57219','D574','D5740','D5741','D57411','D57412','D57419','D578',
		   				       'D5780','D5781','D57811','D57812','D57819')
			and mey.rx_coverage = 1;

--ccaei - Inpatient Admission	   				      
drop table if exists dev.am_scd_ccaei;		   				      
	select distinct *
		into dev.am_scd_ccaei	
		from truven.ccaei c 
		where cast (c.enrolid as text) in (select distinct member_id_src
													from dev.am_scd_members);

--ccaes - Inpatient Services	   				      
drop table if exists dev.am_scd_ccaes;		   				      
	select distinct *
		into dev.am_scd_ccaes	
		from truven.ccaes c 
		where cast (c.enrolid as text) in (select distinct member_id_src
													from dev.am_scd_members);	
													
--ccaeo - Outpatient Services	   				      
drop table if exists dev.am_scd_ccaeo;		   				      
	select distinct *
		into dev.am_scd_ccaeo	
		from truven.ccaeo c 
		where cast (c.enrolid as text) in (select distinct member_id_src
													from dev.am_scd_members);														
													
--ccaef - Facility Header	   				      
drop table if exists dev.am_scd_ccaef;		   				      
	select distinct *
		into dev.am_scd_ccaef	
		from truven.ccaef c 
		where cast (c.enrolid as text) in (select distinct member_id_src
													from dev.am_scd_members);													
													
--ccaed - Outpatient Pharm Claims	   				      
drop table if exists dev.am_scd_ccaed;		   				      
	select distinct *
		into dev.am_scd_ccaed	
		from truven.ccaed c 
		where cast (c.enrolid as text) in (select distinct member_id_src
													from dev.am_scd_members);														
													
--ccaep - Population	   				      

--ccaea - Annual Enrollment Sum	   				      
drop table if exists dev.am_scd_ccaea;		   				      
	select distinct *
		into dev.am_scd_ccaea	
		from truven.ccaea c 
		where cast (c.enrolid as text) in (select distinct member_id_src
													from dev.am_scd_members)
		order by year, seqnum;	

--ccaet - Enrollment Detail	   				 
drop table if exists dev.am_scd_ccaet;		   				      
	select distinct *
		into dev.am_scd_ccaet	
		from truven.ccaet c 
		where cast (c.enrolid as text) in (select distinct member_id_src
													from dev.am_scd_members)
		order by year, seqnum;		



													