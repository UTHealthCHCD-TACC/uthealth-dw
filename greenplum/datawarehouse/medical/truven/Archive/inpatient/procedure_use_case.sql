/*
 * TRUVEN CASES
 * Data is split between mdcrs (inpatient services) and mdcri (inpatient summary)
 * 
 * This case has a single mdcrs record, but 11 additional proc records in single mdcri record
 */
/*
proc1 = 70551, proctyp=1, drg=812, procmod=26, disdate=2011
dx1
icd9	4359	Unspecified transient cerebral ischemia ***
ic10	C4359	Malignant melanoma of other part of trunk

70551
cms	70551	Mri brain stem w/o dye
*/
select *
from truven.mdcrs
where msclmid=910974166
and enrolid=268861401;

/*
--proc2-12 = 9904,8891,70551,72070,73030,99222,99223,99221,99232,99233,99238
--pproc=null

9904  
hcpcs G9904 = Documentation of medical reason(s) for not screening for tobacco use (e.g., limited life expectancy, other medical reason)
cms	G9904 =	Doc med rsn no tbco scrn 

8891
hcpcs	G8891	Documentation of medical reason(s) for most recent ldl-c not under control (e.g., patients with palliative goals for whom treatment of hypertension with standard treatment goals is not clinically appropriate)

70551 (Why listed again???)
cms	70551	Mri brain stem w/o dye

72070
cms	72070	X-ray exam thorac spine 2vws

73030
cms	73030	X-ray exam of shoulder

99222
cms	99222	Initial hospital care

99223
cms	99223	Initial hospital care

99221
cms	99221	Initial hospital care

99232
cms	99232	Subsequent hospital care

99233
cms	99233	Subsequent hospital care

99238
cms	99238	Hospital discharge day
*/
select *
from truven.mdcri
where caseid=239820 and enrolid=268861401;


/*
 * Are there outpatient record with the same proc1???
 */
select enrolid, msclmid, *
from truven.mdcro m 
where proc1='70551'
limit 5;



/*
 enrolid=1782502 and msclmid=890433166, proc1 = man, proctype = 1, 7 or null
 This record has MANY claim detail line items including things like
 svcdate = 2015-04-14
 All dx1 codes are the same, but only found in icd9 table despite svcdate of 2015
 
 dx1
 icd9	78097	Altered mental status Altered mental status
 
 cms    99285   Emergency dept visit
 
 cms	97116	Gait training therapy
  
 cms	J7050	Normal saline solution infus
 hcpcs	J7050	Infusion, normal saline solution, 250 cc
 
 cms	85610	Prothrombin time
 
 
 */
select *
from truven.mdcro m 
where enrolid=1782502 and msclmid=890433166;


@set code='%78097%'

select 'icd9', code, long_description 
from reference_tables.icd_9_diags 
where code like :code
union
select 'ic10', code, description
from reference_tables.icd_10_diags
where code like :code;
union
select 'cms', code, description
from reference_tables.cms_proc_codes 
where code like :code
union
select 'hcpcs', code, full_desc
from reference_tables.hcpcs
where code like :code;