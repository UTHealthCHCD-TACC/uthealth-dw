select 'mdcr', b.month_year_id, a.uth_member_id,
	   c.gender_cd, m.state_code, m.zip_cd, substring(m.zip_cd,1,3),
	   bene_enrollmt_ref_yr::int - extract( year from bene_birth_dt::date),bene_birth_dt::date, bene_death_dt::date,
	   'ABCD' as plan_type, 'MDCR'
from medicare.mbsf_abcd_summary m
  join data_warehouse.dim_member_id_src a
    on a.member_id_src = m.bene_id::text
   and a.data_source = 'mdcr'
  join data_warehouse.ref_month_year b
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
  left outer join data_warehouse.ref_gender c
    on c.data_source = 'mdcr'
   and c.gender_cd_src = m.sex_ident_cd
   ;
   
  
  
  create table data_warehouse.medicare_eligibility_year ( 
  	uth_member_id bigint,
  	enrollment_year int,
  	original_coverage_start date,
  	entitlement_reason_original char(1),
  	entitlement_reason_current char(1),
  	hmo_coverage_months int,
  	ptd_plan_coverage_months int,
  	mdcr_part_b_coverage_months int,
  	rds_coverage_months int,
  	state_buyin_total_months int,
  	bene_hi_cvrage_tot_mons int,
  	dual_eligible_months int
  );
  