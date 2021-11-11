/* ******************************************************************************************************
 *  The member_enrollment_monthly table creates one record for each month/year that a member was enrolled in coverage
 *  Run the relevant code section for the dataset in (---------------- data loads --------------------)
 * 
 *  !!!!!!!!!  data_warehouse.dim_member_id_src table must be populated first !!!!!!!!!    
 *   	             Use dw-create-load-dim_member_id_src.sql in Git    
 * 	
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 1/01/2021 || script created 
 * ******************************************************************************************************
 *  wc002  || 6/28/21 || added logic to exclude enrollment records after death optum dod
 * ******************************************************************************************************
 *  wallingTACC  || 8/23/2021 || Cleaning up comments
 * ******************************************************************************************************
 *  wc003  || 9/02/2021 || Changing process to load dw_staging. Add mapping for null race to assign 0 (Unknown).
 * ******************************************************************************************************
 *  jw001  || 9/20/2021 || Cut to its own script file from longer file
 *  ******************************************************************************************************
 *  ******************************************************************************************************
 *  wc004  || 11/06/2021 || moved table creation to new script. formatting. changed bus_cd mapping
 *  ******************************************************************************************************
*/


----  // BEGIN SCRIPT 
vacuum analyze dw_staging.member_enrollment_monthly;


-- *** Medicare  Texas--------------------------------------------------------------------------------------
insert into dw_staging.member_enrollment_monthly (
	data_source, 
	year, 
	month_year_id, 
	uth_member_id,
	gender_cd, 
	state, 
	zip5, 
	zip3,
	age_derived, 
	dob_derived, 
	death_date,
	plan_type, 
	bus_cd, 
	rx_coverage, 
	fiscal_year, 
	race_cd     
	)		
select 'mcrt',
	   b.year_int, 
	   b.month_year_id, 
	   a.uth_member_id,
	   c.gender_cd,
	   case when e.state_cd is null then 'XX' else e.state_cd end, 
	   m.zip_cd, 
	   substring(m.zip_cd,1,3),
	   bene_enrollmt_ref_yr::int - extract( year from bene_birth_dt::date),bene_birth_dt::date, 
	   bene_death_dt::date,
	   ent.plan_type, 
	   null as bus, 
	   ptd.ptd_coverage, 
	   dev.fiscal_year_func(m.year::int2), 
	   case when r.race_cd is null then '0' else r.race_cd end
from medicare_texas.mbsf_abcd_summary m
  join data_warehouse.dim_uth_member_id a
    on a.member_id_src = m.bene_id::text
   and a.data_source = 'mcrt'
  left outer join reference_tables.ref_gender c
    on c.data_source = 'mcr'
   and c.gender_cd_src = m.sex_ident_cd
  left outer join reference_tables.ref_medicare_state_codes e 
     on e.medicare_state_cd = m.state_code   
  left outer join reference_tables.ref_race r 
     on r.race_cd_src = m.bene_race_cd 
    and r.data_source = 'mcrt'
  join reference_tables.ref_month_year b
    on b.year_int = bene_enrollmt_ref_yr::int
   and 
   (	month_int = case when m.mdcr_status_code_01 in ('10','11','20','21','31') then 1 else 0 end
     or month_int = case when m.mdcr_status_code_02 in ('10','11','20','21','31') then 2 else 0 end
     or month_int = case when m.mdcr_status_code_03 in ('10','11','20','21','31') then 3 else 0 end
     or month_int = case when m.mdcr_status_code_04 in ('10','11','20','21','31') then 4 else 0 end
     or month_int = case when m.mdcr_status_code_05 in ('10','11','20','21','31') then 5 else 0 end
     or month_int = case when m.mdcr_status_code_06 in ('10','11','20','21','31')then 6 else 0 end
     or month_int = case when m.mdcr_status_code_07 in ('10','11','20','21','31') then 7 else 0 end
     or month_int = case when m.mdcr_status_code_08 in ('10','11','20','21','31') then 8 else 0 end
     or month_int = case when m.mdcr_status_code_09 in ('10','11','20','21','31') then 9 else 0 end
     or month_int = case when m.mdcr_status_code_10 in ('10','11','20','21','31') then 10 else 0 end
     or month_int = case when m.mdcr_status_code_11 in ('10','11','20','21','31') then 11 else 0 end
     or month_int = case when m.mdcr_status_code_12 in ('10','11','20','21','31') then 12 else 0 end
    )
  join reference_tables.ref_medicare_entlmt_buyin ent 
    on ent.buyin_cd = case when b.month_int = 1 then m.mdcr_entlmt_buyin_ind_01 
                           when b.month_int = 2 then m.mdcr_entlmt_buyin_ind_02 
                           when b.month_int = 3 then m.mdcr_entlmt_buyin_ind_03 
                           when b.month_int = 4 then m.mdcr_entlmt_buyin_ind_04 
                           when b.month_int = 5 then m.mdcr_entlmt_buyin_ind_05 
                           when b.month_int = 6 then m.mdcr_entlmt_buyin_ind_06 
                           when b.month_int = 7 then m.mdcr_entlmt_buyin_ind_07 
                           when b.month_int = 8 then m.mdcr_entlmt_buyin_ind_08
                           when b.month_int = 9 then m.mdcr_entlmt_buyin_ind_09 
                           when b.month_int = 10 then m.mdcr_entlmt_buyin_ind_10 
                           when b.month_int = 11 then m.mdcr_entlmt_buyin_ind_11 
                           when b.month_int = 12 then m.mdcr_entlmt_buyin_ind_12 
                           else null end      
  join reference_tables.ref_medicare_ptd_cntrct ptd 
    on ptd.ptd_first_char = case when b.month_int = 1 then substring(m.ptd_cntrct_id_01,1,1)
                                 when b.month_int = 2 then substring(m.ptd_cntrct_id_02,1,1)
                                 when b.month_int = 3 then substring(m.ptd_cntrct_id_03,1,1)
                                 when b.month_int = 4 then substring(m.ptd_cntrct_id_04,1,1)
                                 when b.month_int = 5 then substring(m.ptd_cntrct_id_05,1,1)
                                 when b.month_int = 6 then substring(m.ptd_cntrct_id_06,1,1)
                                 when b.month_int = 7 then substring(m.ptd_cntrct_id_07,1,1)
                                 when b.month_int = 8 then substring(m.ptd_cntrct_id_08,1,1)
                                 when b.month_int = 9 then substring(m.ptd_cntrct_id_09,1,1)
                                 when b.month_int = 10 then substring(m.ptd_cntrct_id_10,1,1)
                                 when b.month_int = 11 then substring(m.ptd_cntrct_id_11,1,1)
                                 when b.month_int = 12 then substring(m.ptd_cntrct_id_12,1,1)
                           else null end
;
	



-- Medicare National --------------------------------------------------------------------------------------
insert into dw_staging.member_enrollment_monthly (
	data_source, 
	year, 
	month_year_id, 
	uth_member_id,
	gender_cd, 
	state, 
	zip5, 
	zip3,
	age_derived, 
	dob_derived, 
	death_date,
	plan_type, 
	bus_cd, 
	rx_coverage, 
	fiscal_year, 
	race_cd     
	)		
select 'mcrt',
	   b.year_int, 
	   b.month_year_id, 
	   a.uth_member_id,
	   c.gender_cd,
	   case when e.state_cd is null then 'XX' else e.state_cd end, 
	   m.zip_cd, 
	   substring(m.zip_cd,1,3),
	   bene_enrollmt_ref_yr::int - extract( year from bene_birth_dt::date),bene_birth_dt::date, 
	   bene_death_dt::date,
	   ent.plan_type, 
	   null as bus, 
	   ptd.ptd_coverage, 
	   dev.fiscal_year_func(m.year::int2), 
	   case when r.race_cd is null then '0' else r.race_cd end
from medicare_national.mbsf_abcd_summary m
  join data_warehouse.dim_uth_member_id a
    on a.member_id_src = m.bene_id::text
   and a.data_source = 'mcrn'
  left outer join reference_tables.ref_gender c
    on c.data_source = 'mcr'
   and c.gender_cd_src = m.sex_ident_cd
  left outer join reference_tables.ref_medicare_state_codes e 
     on e.medicare_state_cd = m.state_code   
  left outer join reference_tables.ref_race r 
     on r.race_cd_src = m.bene_race_cd 
    and r.data_source = 'mcrn'
  join reference_tables.ref_month_year b
    on b.year_int = bene_enrollmt_ref_yr::int
   and 
   (	month_int = case when m.mdcr_entlmt_buyin_ind_01 in ('1','3','A','C') then 1 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_02 in ('1','3','A','C') then 2 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_03 in ('1','3','A','C') then 3 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_04 in ('1','3','A','C') then 4 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_05 in ('1','3','A','C') then 5 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_06 in ('1','3','A','C') then 6 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_07 in ('1','3','A','C') then 7 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_08 in ('1','3','A','C') then 8 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_09 in ('1','3','A','C') then 9 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_10 in ('1','3','A','C') then 10 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_11 in ('1','3','A','C') then 11 else 0 end
     or month_int = case when m.mdcr_entlmt_buyin_ind_12 in ('1','3','A','C') then 12 else 0 end
    )
  join reference_tables.ref_medicare_entlmt_buyin ent 
    on ent.buyin_cd = case when b.month_int = 1 then m.mdcr_entlmt_buyin_ind_01 
                           when b.month_int = 2 then m.mdcr_entlmt_buyin_ind_02 
                           when b.month_int = 3 then m.mdcr_entlmt_buyin_ind_03 
                           when b.month_int = 4 then m.mdcr_entlmt_buyin_ind_04 
                           when b.month_int = 5 then m.mdcr_entlmt_buyin_ind_05 
                           when b.month_int = 6 then m.mdcr_entlmt_buyin_ind_06 
                           when b.month_int = 7 then m.mdcr_entlmt_buyin_ind_07 
                           when b.month_int = 8 then m.mdcr_entlmt_buyin_ind_08
                           when b.month_int = 9 then m.mdcr_entlmt_buyin_ind_09 
                           when b.month_int = 10 then m.mdcr_entlmt_buyin_ind_10 
                           when b.month_int = 11 then m.mdcr_entlmt_buyin_ind_11 
                           when b.month_int = 12 then m.mdcr_entlmt_buyin_ind_12 
                           else null end      
  join reference_tables.ref_medicare_ptd_cntrct ptd 
    on ptd.ptd_first_char = case when b.month_int = 1 then substring(m.ptd_cntrct_id_01,1,1)
                                 when b.month_int = 2 then substring(m.ptd_cntrct_id_02,1,1)
                                 when b.month_int = 3 then substring(m.ptd_cntrct_id_03,1,1)
                                 when b.month_int = 4 then substring(m.ptd_cntrct_id_04,1,1)
                                 when b.month_int = 5 then substring(m.ptd_cntrct_id_05,1,1)
                                 when b.month_int = 6 then substring(m.ptd_cntrct_id_06,1,1)
                                 when b.month_int = 7 then substring(m.ptd_cntrct_id_07,1,1)
                                 when b.month_int = 8 then substring(m.ptd_cntrct_id_08,1,1)
                                 when b.month_int = 9 then substring(m.ptd_cntrct_id_09,1,1)
                                 when b.month_int = 10 then substring(m.ptd_cntrct_id_10,1,1)
                                 when b.month_int = 11 then substring(m.ptd_cntrct_id_11,1,1)
                                 when b.month_int = 12 then substring(m.ptd_cntrct_id_12,1,1)
                           else null end
;


----/END SCRIPT