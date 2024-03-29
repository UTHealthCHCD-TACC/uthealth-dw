/* ******************************************************************************************************
 *  Run just once to load reference_tables with static data
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 1/01/2021 || script created 
 * ******************************************************************************************************
 *  wc002  || 10/25/2021 || employee status 
 * ******************************************************************************************************
 *  wc003  || 11/04/2021 || update bus_cd table
 * ******************************************************************************************************
 * 	sa001  || 11/30/2023 || update plan_type and bus_cd tables for IQVIA
 ********************************************************************************************************
 */

-------------------------------------------------------------------------------------------------------
---- Tables Loaded from csv or other source files
-------------------------------------------------------------------------------------------------------
				
create table data_warehouse.ref_truven_state_codes (truven_code int2, state varchar, abbr text);			

create table data_warehouse.ref_admit_type (admit_type_cd char(1), admit_type varchar, admit_type_desc varchar);

create table data_warehouse.ref_admit_source (admit_source_cd char(5), admit_source varchar, admit_source_desc varchar);

create table data_warehouse.ref_medicare_state_codes (medicare_state_cd char(2), state varchar, state_cd char(2));
-------------------------------------------------------------------------------------------------------
---- Tables loaded with a hardcoded SQL insert statement
-------------------------------------------------------------------------------------------------------


--- Data Source table
drop table data_warehouse.ref_data_source ; 

create table data_warehouse.ref_data_source
(
	data_source char(4),
	data_source_desc text,
	fiscal_year_flag bool,
	fiscal_year_begin_month int
)
;

insert into data_warehouse.ref_data_source (data_source, data_source_cd, data_source_desc, fiscal_year_flag, fiscal_year_begin_month)
       values ('optz','Optum Zip',false,1),
   			  ('optd','Optum Date of Death',false,1),
   			  ('truv','Truven',false,1),
   			  ('mcrt','Medicare Texas',false,1),
   			  ('mcrn','Medicare National',false,1),
   			  ('mdcd','Medicaid Texas',true,10)
   			  ;
         	

--- gender decode table
create table data_warehouse.ref_gender 
(
	data_source char(4), 
	gender_cd_src char(5),
	gender_cd char(1)
)
;

delete from data_warehouse.ref_gender;

insert into data_warehouse.ref_gender (data_source, gender_cd_src, gender_cd)
       values ('trv','1','M'),
              ('trv','2','F'),
              ('opt','M','M'),
              ('opt','F','F'),
              ('opt','U','U'), 
              ('mdcr','0','U'),
              ('mdcr','1','M'),
              ('mdcr','2','F') ;

             
             
---plan type decode table   			 
create table reference_tables.ref_plan_type (
				data_source char(4), 
				source_column_name text, 
				plan_type_src varchar, 
				plan_type char(4), 
				plan_desc text
				);
				
delete from reference_tables.ref_plan_type;				
				
insert into reference_tables.ref_plan_type (data_source, source_column_name, plan_type_src, plan_type, plan_desc)
		values ('trv','plantyp','1','BMM','basic major medical'),
			   ('trv','plantyp','2','CMP','comprehensive'),
			   ('trv','plantyp','3','EPO',''),
			   ('trv','plantyp','4','HMO',''),
			   ('trv','plantyp','5','POS',''),
			   ('trv','plantyp','6','PPO',''),
			   ('trv','plantyp','7','POS','pos with capitation'),
			   ('trv','plantyp','8','CDHP',''),
			   ('trv','plantyp','9','HDHP',''),
			   ('opt','product','ALL','ALL',''),
			   ('opt','product','EPO','EPO',''),
			   ('opt','product','GPO','GPO',''),
			   ('opt','product','HMO','HMO',''),
			   ('opt','product','IND','IND',''),
			   ('opt','product','IPP','IPP',''),
			   ('opt','product','NONE','NONE',''),
			   ('opt','product','OTH','OTH',''),
			   ('opt','product','POS','POS',''),
			   ('opt','product','PPO','PPO',''),
			   ('opt','product','SPN','SPN',''),
			   ('opt','product','UNK','UNK',''),
			   ('iqva','prd_type','D','CDHP','Consumer Directed Health Care'),
			   ('iqva','prd_type','H','HMO','Health Maintenance Organization'),
			   ('iqva','prd_type','I','FFS','Indemnity/Traditional'),
			   ('iqva','prd_type','P','PPO','Preferred Provider Organization'),
			   ('iqva','prd_type','R','HSA','Health Savings Account (HSA)'),
			   ('iqva','prd_type','S','POS','Point of Service'),
			   ('iqva','prd_type','U','UNK','Unknown/Missing')
			  ;
				
select *--distinct year, medadv 
from truven.ccaea;
			  
			  
--- business code decode table wc003
drop table if exists reference_tables.ref_bus_cd;

create table reference_tables.ref_bus_cd ( data_source char(4), bus_cd char(4), bus_desc text, note text );

insert into reference_tables.ref_bus_cd (data_source, bus_cd, bus_desc, note)
	   values ('truv','MS','Medicare Supplemental','from mdcr tables where medadv is null or 0'),
	          ('truv','COM','Commercial','from ccae tables where medadv is null or 0'),
	          ('truv','MA','Medicare Advantage','from mdcr or ccae tables where medadv = 1'),
	          ('mcrt',null,'Medicare',null),
	          ('mcrn',null,'Medicare',null),
	          ('optz','COM','Commercial','from mbr_enroll.bus'),
	          ('optz','MA','Medicare Advantage','from mbr_enroll.bus'),
	          ('optd','COM','Commercial','from mbr_enroll_r.bus'),
	          ('optd','MA','Medicare Advantage','from mbr_enroll_r.bus'),
	          ('mdcd',null,'Medicaid',null),
	          ('iqva','COM','Commercial','from iqvia.enroll_synth where pay_type = C'),
			  ('iqva','CHIP','State Childrens Health Insurance Program (SCHIP)','from iqvia.enroll_synth where pay_type = K'),
			  ('iqva','MDCD','Medicaid','from iqvia.enroll_synth where pay_type = M'),
			  ('iqva','MA','Medicare Risk (presently known as Medicare Advantage)','from iqvia.enroll_synth where pay_type = R'),
			  ('iqva','SI','Self-Insured','from iqvia.enroll_synth where pay_type = S'),
			  ('iqva','MS','Medicare Cost (Medicare Supplemental)','from iqvia.enroll_synth where pay_type = T'),
			  ('iqva',null,'Unknown/Missing','from iqvia.enroll_synth where pay_type = U')
			 ;
 
---ref_race
create table reference_tables.ref_race (data_source char(4), race_cd_src text, race_cd char(1), race_desc text );     
       

insert into reference_tables.ref_race values  ('mcrn','1','1','White'), 
											  ('mcrn','2','2','Black'), 
											  ('mcrn','3','3','Other'), 
											  ('mcrn','4','4','Asian'), 
											  ('mcrn','5','5','Hispanic'), 
											  ('mcrn','6','6','North American Native'), 
											  ('mcrn','0','0','Unknown'), 
											  ('mcrt','1','1','White'), 
											  ('mcrt','2','2','Black'), 
											  ('mcrt','3','3','Other'), 
											  ('mcrt','4','4','Asian'), 
											  ('mcrt','5','5','Hispanic'), 
											  ('mcrt','6','6','North American Native'), 
											  ('mcrt','0','0','Unknown'), 
											  ('optd','W','1','White'), 
											  ('optd','B','2','Black'), 
											  ('optd','H','5','Hispanic'), 
											  ('optd','A','4','Asian'), 
											  ('optd','0','0','Unknown'),
											  ('mdcd','1','1','White'),
											  ('mdcd','2','2','Black'),
											  ('mdcd','3','5','Hispanic'), 
											  ('mdcd','4','6','North American Native'),
											  ('mdcd','5','4','Asian'), 
											  ('mdcd','6','0','Unknown'), 
											  ('optz',null,'0','Unknown'), 
											  ('optd',null,'0','Unknown'), 
											  ('mcrt',null,'0','Unknown'), 
											  ('mcrn',null,'0','Unknown'), 
											  ('mdcd',null,'0','Unknown'), 
											  ('truv',null,'0','Unknown');
											 
											 
											 
											 
---medicare enrollment ref tables 
create table ref_medicare_ptd_cntrct (ptd_first_char char(1), ptd_coverage int2);


---**wcc002

---employee status based on truven data dictionary eestatu
create table reference_tables.ref_employee_status (employee_status int2, employee_status_desc text);


insert into reference_tables.ref_employee_status values (1, 'Active Full Time'),
                                                        (2, 'Active Part Time'),
                                                        (3, 'Early Retiree'),
                                                        (4, 'Medicare Eligible Retiree'),
                                                        (5, 'Retiree (status unknown)'),
                                                        (6, 'COBRA'),
                                                        (7, 'Long Term Disability'),
                                                        (8, 'Surviving Spouse/Depend'),
                                                        (9, 'Other/Unknown');

                                                       
---** end wcc002									
		