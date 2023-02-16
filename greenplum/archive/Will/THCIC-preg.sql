--insert into stage.dbo.WC_THCIC_PREG_FACILITY_TYPE
select  THCIC_ID , PROVIDER_NAME , FAC_TEACHING_IND 
into stage.dbo.WC_THCIC_PREG_FACILITY_TYPE
from THCIC_NEW.dbo.ip
where PROVIDER_NAME in ( select PROVIDER_NAME from stage.dbo.WC_THCIC_PREG_IP_BASE1)


select * --a.RECORD_ID, A.UNITS_OF_SERVICE, a.CHRGS_LINE_ITEM
from THCIC_NEW.dbo.IP_PUDF_charges_2018 a
where RECORD_ID = '120180012858'

select lefT(right(RECORD_ID,11),4) , count(*) from stage.dbo.WC_THCIC_PREG_IP_BASE2 group by lefT(right(RECORD_ID,11),4)

--insert into stage.dbo.WC_THCIC_PREG_IP_BASE2
select RECORD_ID, BLOOD_AMOUNT, BLOOD_ADM_AMOUNT, 
       VALUE_CODE_1, VALUE_CODE_2, VALUE_CODE_3, VALUE_CODE_4, VALUE_CODE_5, VALUE_CODE_6, 
       VALUE_CODE_7, VALUE_CODE_8, VALUE_CODE_9, VALUE_CODE_10, VALUE_CODE_11, VALUE_CODE_12,
       VALUE_AMOUNT_1, VALUE_AMOUNT_2, VALUE_AMOUNT_3, VALUE_AMOUNT_4, VALUE_AMOUNT_5, VALUE_AMOUNT_6, 
       VALUE_AMOUNT_7, VALUE_AMOUNT_8, VALUE_AMOUNT_9, VALUE_AMOUNT_10, VALUE_AMOUNT_11, VALUE_AMOUNT_12
--into stage.dbo.WC_THCIC_PREG_IP_BASE2
from THCIC_NEW.dbo.IP_PUDF_base2_2012 a
where RECORD_ID in ( select record_id from stage.dbo.WC_THCIC_PREG_IP_BASE1 )


--not sure if this is useful
insert into stage.dbo.WC_THCIC_PREG_PUDF_CHARGES
select RECORD_ID, REVENUE_CODE, HCPCS_QUALIFIER, HCPCS_PROCEDURE_CODE, MODIFIER_1, MODIFIER_2, MODIFIER_3, MODIFIER_4, UNIT_MEASUREMENT_CODE, 
       UNITS_OF_SERVICE, UNIT_RATE,CHRGS_LINE_ITEM, CHRGS_NON_COV
--into stage.dbo.WC_THCIC_PREG_PUDF_CHARGES
from THCIC_NEW.dbo.IP_PUDF_charges_2018
where RECORD_ID in ( select record_id from stage.dbo.WC_THCIC_PREG_IP_BASE1 )



select * from THCIC_NEW.dbo.IP_PUDF_charges_2016

----main file
insert into stage.dbo.WC_THCIC_PREG_IP_BASE1
select RECORD_ID, SEX_CODE, PAT_AGE, PAT_ZIP, a.PAT_COUNTY , RACE, ETHNICITY ,FIRST_PAYMENT_SRC, PROVIDER_NAME,
       ADMITTING_DIAGNOSIS, PRINC_DIAG_CODE, OTH_DIAG_CODE_1, OTH_DIAG_CODE_2, OTH_DIAG_CODE_3, OTH_DIAG_CODE_4, OTH_DIAG_CODE_5, OTH_DIAG_CODE_6,
       OTH_DIAG_CODE_7, OTH_DIAG_CODE_8, OTH_DIAG_CODE_9, OTH_DIAG_CODE_10, OTH_DIAG_CODE_11, OTH_DIAG_CODE_12, OTH_DIAG_CODE_13,
       OTH_DIAG_CODE_14, OTH_DIAG_CODE_15, OTH_DIAG_CODE_16, OTH_DIAG_CODE_17, OTH_DIAG_CODE_18, OTH_DIAG_CODE_19,
       OTH_DIAG_CODE_20, OTH_DIAG_CODE_21, OTH_DIAG_CODE_22, OTH_DIAG_CODE_23, OTH_DIAG_CODE_24,
       PRINC_SURG_PROC_CODE, OTH_SURG_PROC_CODE_1, OTH_SURG_PROC_CODE_2, OTH_SURG_PROC_CODE_3, OTH_SURG_PROC_CODE_4,
       OTH_SURG_PROC_CODE_5, OTH_SURG_PROC_CODE_6, OTH_SURG_PROC_CODE_7, OTH_SURG_PROC_CODE_8, OTH_SURG_PROC_CODE_9,
       OTH_SURG_PROC_CODE_10, OTH_SURG_PROC_CODE_11, OTH_SURG_PROC_CODE_12, OTH_SURG_PROC_CODE_13, OTH_SURG_PROC_CODE_14,
       OTH_SURG_PROC_CODE_15, OTH_SURG_PROC_CODE_16, OTH_SURG_PROC_CODE_17, OTH_SURG_PROC_CODE_18, OTH_SURG_PROC_CODE_19,
       OTH_SURG_PROC_CODE_20, OTH_SURG_PROC_CODE_21, OTH_SURG_PROC_CODE_22, OTH_SURG_PROC_CODE_23, OTH_SURG_PROC_CODE_24
-- into stage.dbo.WC_THCIC_PREG_IP_BASE1
from THCIC_NEW.dbo.IP_PUDF_base1_2018 a
where  a.SEX_CODE in ('F','U') and 
   (    a.APR_DRG in ('370','371','372','373','374','375') 
        or a.ADMITTING_DIAGNOSIS in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.PRINC_DIAG_CODE in ( select dx_code from stage.dbo.wc_thcic_preg_diags ) 
        or a.OTH_DIAG_CODE_1 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.OTH_DIAG_CODE_2 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.OTH_DIAG_CODE_3 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.OTH_DIAG_CODE_4 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.OTH_DIAG_CODE_5 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.OTH_DIAG_CODE_6 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.OTH_DIAG_CODE_7 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.OTH_DIAG_CODE_8 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.OTH_DIAG_CODE_9 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.OTH_DIAG_CODE_10 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.OTH_DIAG_CODE_11 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.OTH_DIAG_CODE_12 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.OTH_DIAG_CODE_13 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.OTH_DIAG_CODE_14 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.OTH_DIAG_CODE_15 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.OTH_DIAG_CODE_16 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.OTH_DIAG_CODE_17 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.OTH_DIAG_CODE_18 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.OTH_DIAG_CODE_19 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.OTH_DIAG_CODE_20 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.OTH_DIAG_CODE_21 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.OTH_DIAG_CODE_22 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.OTH_DIAG_CODE_23 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.OTH_DIAG_CODE_24 in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
        or a.PRINC_SURG_PROC_CODE in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_1 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_2 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_3 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_4 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_5 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_6 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_7 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_8 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_9 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_10 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_11 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_12 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_13 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_14 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_15 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_16 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_17 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_18 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_19 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_20 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_21 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_22 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_23 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
        or a.OTH_SURG_PROC_CODE_24 in (select proc_cd from STAGE.dbo.wc_thcic_preg_procs)
      )



---dx
drop table stage.dbo.wc_thcic_preg_diags
      
select distinct admitting_diagnosis as dx_code 
into stage.dbo.wc_thcic_preg_diags
from THCIC_NEW.dbo.IP_PUDF_base1_2012 ipb where ADMITTING_DIAGNOSIS between 'O60' and 'O779'

--walk through the years go grab all the diag codes 
insert into stage.dbo.wc_thcic_preg_diags
select distinct admitting_diagnosis 
from THCIC_NEW.dbo.IP_PUDF_base1_2012 ipb where ( ADMITTING_DIAGNOSIS between 'O60' and 'O779'
or ADMITTING_DIAGNOSIS between '65100' and '65993'
or ADMITTING_DIAGNOSIS between '66200' and '66213'
or ADMITTING_DIAGNOSIS between '660' and '669'
or ADMITTING_DIAGNOSIS in ('O81','650','V270','Z370','Z370','O80','O82','O630','O631','O639')
)
and ADMITTING_DIAGNOSIS not in ( select dx_code from stage.dbo.wc_thcic_preg_diags )
;


--proc
create table STAGE.dbo.wc_thcic_preg_procs (proc_cd varchar(50));


insert into STAGE.dbo.wc_thcic_preg_procs  values ('72'),('721'),('7221'),('7229'),('7231'),('7239'),('724'),('726'),('7251'),('7252'),
		('7253'),('7254'),('7271'),('7279'),('728'),('729'),('7322'),('7359'),('736'),('74'),('741'),('742'),('743'),('744'),('7499'),('10D07Z3'),
		('10D07Z4'),('10D07Z5'),('10S07ZZ'),('10D07Z3'),('10S07ZZ'),('10D07Z8 '),('10D07Z7'),('10E0XZZ'),('0W8NXZZ'),('10D00Z0 '),('10D00Z1'),
		('10D00Z2'),('10D00Z0 ')
;





