

--- gender decode table
create table reference_tables.ref_gender 
(
	data_source char(4), 
	gender_cd_src char(5),
	gender_cd char(1)
)
;

delete from reference_tables.ref_gender;

insert into reference_tables.ref_gender (data_source, gender_cd_src, gender_cd)
       values ('trv','1','M'),
              ('trv','2','F'),
              ('opt','M','M'),
              ('opt','F','F'),
              ('opt','U','U') ;

             
             

--- Data Source shorthand table

drop table reference_tables.ref_data_source ;            
create table reference_tables.ref_data_source
(
	data_source char(4),
	data_source_cd smallint,
	data_source_desc text
)
;

insert into reference_tables.ref_data_source (data_source, data_source_cd, data_source_desc)
       values ('optz',10,'Optum Zip'),
   			  ('optd',20,'Optum Date of Death'),
   			  ('trvc',30,'Truven Commercial'),
   			  ('trvm',30,'Truven Medicare'),  --- the 30 is intentional, truven commercial and medicare members should have the same ID
   			  ('bcbs',40,'BlueCross BlueShield'),
   			  ('mdcr',50,'Medicare'),
   			  ('mdcd',60,'Medicaid'),
   			  ('cern',70,'Cerner')	       ;
       
   			 
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
			   ('opt','product','UNK','UNK','')
			   ;
				
				
	create table reference_tables.ref_truven_state_codes (truven_code int2, state varchar, abbr text);			






create table dev.hpm_cohorts 
( enrolid varchar, industry_type varchar, age_group varchar, gender varchar, egeoloc varchar, diabetes_flag char(1), sample_year char(4) );




