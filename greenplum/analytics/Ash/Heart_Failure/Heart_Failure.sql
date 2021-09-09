drop table if exists dev.am_hf_optd_members;	
	select distinct m.member_id_src 
		into dev.am_hf_optd_members
		from optum_zip.diagnostic d, data_warehouse.dim_uth_member_id m, data_warehouse.member_enrollment_yearly y
		where d.patid::text = m.member_id_src
			and m.uth_member_id = y.uth_member_id 
			and y.age_derived between 18 and 85
			and y.data_source = 'optd'
			and diag in ('I501', 'I502', 'I504', 'I508', 'I509')		
			and d."year" in (2016, 2017, 2018, 2019)
			and y."year" in (2016, 2017, 2018, 2019);

/*
	select distinct y.member_id_src 
		into dev.am_hf_optd_members
		from optum_zip.diagnostic d, data_warehouse.member_enrollment_yearly y
		where d.patid::text = y.member_id_src 
			and y.age_derived between 18 and 85
			and y.data_source = 'optd'
			and diag in ('I501', 'I502', 'I504', 'I508', 'I509')		
			and d."year" in (2016, 2017, 2018, 2019)
			and y."year" in (2016, 2017, 2018, 2019);
*/
--select count(*) from dev.am_hf_optd_members;
-----------------------------------------------------------------------------------------------------------------------------
--get enrollment 	
drop table if exists dev.am_hf_optd_member_enrollment;
	select distinct patid, pat_planid, aso, bus, cdhp, eligeff, eligend, gdr_cd, group_nbr, health_exch, 
		lis_dual, product, race, state, yrdob, extract_ym, "version"
	--select distinct e.patid, e.eligeff , e.eligend , e.gdr_cd , e.race , e.yrdob , e.extract_ym , e.lis_dual , 
	--		e.state ,  e.bus , e.health_exch , e."version" 
		into dev.am_hf_optd_member_enrollment
		from optum_zip.mbr_enroll_r e
		inner join dev.am_hf_optd_members m on e.patid::text = m.member_id_src
		where date_part('year', e.eligeff) in (2015, 2016, 2017, 2018, 2019)
			or date_part('year', e.eligend) in (2015, 2016, 2017, 2018, 2019)
		order by e.patid , e.eligeff;

--select count(*) from dev.am_hf_optd_member_enrollment;
-----------------------------------------------------------------------------------------------------------------------------	
--get medical	
drop table if exists dev.am_hf_optd_medical;
	SELECT distinct patid, pat_planid, admit_chan, admit_type, bill_prov, charge, clmid, clmseq, cob, coins, conf_id, 
		copay, deduct, drg, dstatus, enctr, fst_dt, hccc, icd_flag, loc_cd, lst_dt, ndc, paid_dt, paid_status, pos, 
		proc_cd, procmod, prov, prov_par, provcat, refer_prov, rvnu_cd, service_prov, std_cost, std_cost_yr, tos_cd, 
		units, extract_ym, "version", alt_units, bill_type, ndc_uom, ndc_qty, op_visit_id, procmod2, procmod3, procmod4, tos_ext
	--select distinct d.patid , d.clmid , d.charge , d.coins , d.copay , d.deduct , d.drg , d.dstatus , d.enctr , d.fst_dt , d.icd_flag , 
	--		d.loc_cd , d.lst_dt , d.ndc , d.paid_status , d.provcat , d.rvnu_cd , d.std_cost , d.ndc_uom , d.ndc_qty , 
	--		d.admit_chan , d.admit_type , d.bill_prov , d.conf_id , d.paid_dt
		into dev.am_hf_optd_medical	
		from optum_zip.medical d 
		inner join dev.am_hf_optd_members m on d.patid::text = m.member_id_src
		where d.year in (2015, 2016, 2017, 2018, 2019);

--select count(*) from dev.am_hf_optd_medical;
-----------------------------------------------------------------------------------------------------------------------------
--get procedure
drop table if exists dev.am_hf_optd_procedure;	
	SELECT distinct p.patid, p.pat_planid, p.clmid, p.icd_flag, proc, proc_position, p.extract_ym, p."version", p.fst_dt
--	select distinct p.patid , p.clmid , p.proc , p.proc_position , p.fst_dt 
		into dev.am_hf_optd_procedure
		from optum_zip.procedure p, dev.am_hf_optd_medical m
		where p.clmid = m.clmid
			and p.patid  = m.patid; 
-----------------------------------------------------------------------------------------------------------------------------
--get diagnositics
drop table if exists dev.am_hf_optd_diagnostic;
	SELECT distinct d.patid, d.pat_planid, d.clmid, diag, diag_position, d.icd_flag, d.loc_cd, poa, d.extract_ym, d."version", d.fst_dt
	--select distinct d.patid , d.clmid , d.diag , d.diag_position , d.poa , d.fst_dt , d.extract_ym 
		into dev.am_hf_optd_diagnostic
		from optum_zip.diagnostic d , dev.am_hf_optd_medical m
		where d.clmid = m.clmid
			and d.patid  = m.patid; 
 
--select count(*) from dev.am_hf_optd_diagnostic;	
-----------------------------------------------------------------------------------------------------------------------------
--get confinement
drop table if exists dev.am_hf_optd_confinement;
	select distinct patid, pat_planid, admit_date, charge, coins, conf_id, copay, deduct, diag1, diag2, diag3, diag4, diag5, 
			disch_date, drg, dstatus, icd_flag, ipstatus, los, pos, proc1, proc2, proc3, proc4, proc5, prov, std_cost, 
			std_cost_yr, tos_cd, extract_ym, "version", icu_ind, icu_surg_ind, maj_surg_ind, maternity_ind, newborn_ind, tos
	--select distinct c.patid, c.admit_date , c.charge , c.coins , c.conf_id , c.copay , c.deduct , 
	--		c.diag1 , c.diag2 , c.diag3 , c.diag4 , c.diag5 , c.disch_date , c.drg , c.dstatus , c.icd_flag ,
	--		c.ipstatus , c.los , c.pos , c.proc1 , c.proc2 , c.proc3 , c.proc4 , c.proc5 , c.std_cost , c.std_cost_yr ,
	--		c.icu_ind , c.icu_surg_ind , c.maj_surg_ind , c.tos_cd 
		into dev.am_hf_optd_confinement
		from optum_zip.confinement c  
		inner join dev.am_hf_optd_members m on c.patid::text = m.member_id_src
		where c.year in (2015, 2016, 2017, 2018, 2019);

--select count(*) from dev.am_hf_optd_confinement;	
-----------------------------------------------------------------------------------------------------------------------------
--get rx
drop table if exists dev.am_hf_optd_rx;
	select distinct patid, pat_planid, ahfsclss, avgwhlsl, brnd_nm, charge, chk_dt, clmid, copay, daw, days_sup, dea, deduct, dispfee, 
			fill_dt, form_ind, form_typ, fst_fill, gnrc_ind, gnrc_nm, mail_ind, ndc, npi, pharm, prc_typ, quantity, rfl_nbr, spclt_ind, 
			specclss, std_cost, std_cost_yr, strength, extract_ym, "version", prescriber_prov, prescript_id
--	select distinct r.patid , r.ahfsclss , r.avgwhlsl , r.brnd_nm , r.charge , r.chk_dt , r.copay , r.daw , r.days_sup ,
--			r.deduct , r.dispfee , r.fill_dt , r.form_ind , r.form_typ , r.fst_fill , r.gnrc_ind , r.mail_ind , r.ndc ,
--			r.pharm , r.prc_typ , r.quantity , r.rfl_nbr , r.spclt_ind , r.specclss , r.std_cost , r.std_cost_yr , 
--			r.strength , r.extract_ym , r."version" 
		into dev.am_hf_optd_rx	
		from optum_zip.rx r 
		inner join dev.am_hf_optd_members m on r.patid::text = m.member_id_src
		where r.year in (2015, 2016, 2017, 2018, 2019);
	
--select count(*) from dev.am_hf_optd_rx;	
-----------------------------------------------------------------------------------------------------------------------------		
--get lab_result		
drop table if exists dev.am_hf_optd_lab_result;
	SELECT distinct patid, pat_planid, abnl_cd, anlytseq, fst_dt, hi_nrml, labclmid, loinc_cd, low_nrml, proc_cd, rslt_nbr, 
			rslt_txt, rslt_unit_nm, tst_desc, tst_nbr, extract_ym, "version"
	--select distinct r.patid , r.abnl_cd , r.anlytseq , r.hi_nrml , r.loinc_cd , r.low_nrml , r.rslt_nbr , r.rslt_txt , 
	--		r.rslt_unit_nm , r.fst_dt 
		into dev.am_hf_optd_lab_result	
		from optum_zip.lab_result r
		inner join dev.am_hf_optd_members m on r.patid::text = m.member_id_src
		where r.year in (2015, 2016, 2017, 2018, 2019);
		
--select count(*) from dev.am_hf_optd_lab_result;	
-----------------------------------------------------------------------------------------------------------------------------			
--get lu_ndc
drop table if exists dev.am_hf_optd_lu_ndc;		
	select distinct ahfsclss, ahfsclss_desc, brnd_nm, dosage_fm_desc, drg_strgth_desc, drg_strgth_nbr, drg_strgth_unit_desc, 
			drg_strgth_vol_nbr, drg_strgth_vol_unit_desc, gnrc_ind, gnrc_nbr, gnrc_nm, gnrc_sqnc_nbr, ndc, ndc_drg_row_eff_dt, 
			ndc_drg_row_end_dt, usc_id, usc_med_desc
--	select distinct r.brnd_nm , r.dosage_fm_desc , r.drg_strgth_desc , r.drg_strgth_nbr , r.drg_strgth_unit_desc ,
--			r.drg_strgth_vol_nbr , r.drg_strgth_vol_unit_desc , r.gnrc_ind , r.gnrc_nbr , r.gnrc_nm , r.ndc 
		into dev.am_hf_optd_lu_ndc	
		from optum_zip.lu_ndc r	;
	
--select count(*) from dev.am_hf_optd_lu_ndc;	
-----------------------------------------------------------------------------------------------------------------------------			
--get lu_diagnosis
drop table if exists dev.am_hf_optd_lu_diagnosis;		
	SELECT diag_cd, diag_desc, diag_fst3_cd, diag_fst3_desc, diag_fst4_cd, diag_fst4_desc, gdr_spec_cd, mdc_cd_desc, 
		mdc_code, icd_ver_cd
--	select distinct r.diag_cd , r.diag_desc , r.mdc_cd_desc , r.mdc_code , r.icd_ver_cd 
		into dev.am_hf_optd_lu_diagnosis	
		from optum_zip.lu_diagnosis r;
	
--select count(*) from dev.am_hf_optd_lu_ndc;			
		
------------------------------------------------------------------------------------------------
/* 
	
select d."year" , count(distinct y.member_id_src) as distinct_member_id_src_per_year
	from optum_zip.diagnostic d , data_warehouse.member_enrollment_yearly y
	where d.patid::text = y.member_id_src 
		and y.age_derived between 18 and 85
		and diag in ('I501', 'I502', 'I504', 'I508', 'I509')
		and d."year" in (2016, 2017, 2018, 2019)
		and y."year" in (2016, 2017, 2018, 2019)
	group by d."year" 
	order by d.year	
	
select d.year, d.data_source , count(distinct d.uth_member_id) as distinct_uth_member_id
	from data_warehouse.claim_diag d, data_warehouse.member_enrollment_yearly y
	where d.uth_member_id = y.uth_member_id 
		and y.age_derived between 18 and 85
		and diag_cd in ('I501', 'I502', 'I504', 'I508', 'I509')
		and d.data_source in('optd', 'optz')
		and d."year" in (2016, 2017, 2018, 2019)
		and y."year" in (2016, 2017, 2018, 2019)
	group by d."year" , d.data_source 
	order by d.data_source , d.year	
	
*/
  