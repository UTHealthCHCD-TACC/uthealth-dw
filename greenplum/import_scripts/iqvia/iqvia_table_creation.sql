
-- Create iqvia.enroll_synth table:

create table if not exists iqvia.enroll_synth(
	der_sex	varchar(1) null,
	der_yob	varchar(4) null,
	pat_id varchar(16) null,
	pat_region varchar(2) null,
	pat_state varchar(2) null,
	pat_Zip3 varchar(3) null,
	grp_indv_cd	varchar(1) null,
	mh_cd varchar(1) null,
	enr_rel	varchar(2) null
	)
	with(
		appendonly = true,
		orientation = column,
		compresstype = zlib
		)
	distributed by (pat_id);
	



-- Create iqvia.enroll2 table:

create table if not exists iqvia.enroll2(
	"year" int2 null,
	pat_id	varchar(16) null,
	mstr_enroll_cd varchar(1) null,
	prd_type varchar(1) null,
	pay_type varchar(1) null,
	pcob_type varchar(1) null,
	mcob_type varchar(1) null,
	month_id varchar(6) null
	)
	with(
		appendonly = true,
		orientation = column,
		compresstype = zlib
		)
	distributed by (pat_id);




-- Create iqvia.claims table:

create table if not exists iqvia.claims (
	"year" int2 null,
	pat_id varchar(16) null,
	claimno varchar(16) null,
	linenum	varchar(20) null, 
	rectype	varchar(1) null,
	tos_flag varchar(1) null,
	pos	varchar(2) null,
	conf_num varchar(16) null,
	patstat varchar(2) null,
	billtype varchar(3) null,
	ndc	varchar(11) null,
	daw	varchar(1) null, 
	formulary varchar(1) null,
	dayssup varchar(8) null, 
	quan varchar(20) null, 
	proc_cde varchar(6) null,
	cpt_mod	varchar(2) null,
	rev_code varchar(4) null,
	srv_unit varchar(20) null,
	from_dt	varchar(10) null, 
	to_dt varchar(10) null,
	diagprc_ind varchar(2) null,
	diag_admit	varchar(7) null,
	diag1 varchar(7) null,
	diag2 varchar(7) null,
	diag3 varchar(7) null,
	diag4 varchar(7) null,
	diag5 varchar(7) null,
	diag6 varchar(7) null,
	diag7 varchar(7) null,
	diag8 varchar(7) null,
	diag9 varchar(7) null,
	diag10 varchar(7) null,
	diag11 varchar(7) null,
	diag12 varchar(7) null,
	icdprc1 varchar(7) null,
	icdprc2 varchar(7) null,
	icdprc3	varchar(7) null,
	icdprc4 varchar(7) null,
	icdprc5	varchar(7) null,
	icdprc6	varchar(7) null,
	icdprc7	varchar(7) null,
	icdprc8	varchar(7) null,
	icdprc9	varchar(7) null,
	icdprc10 varchar(7) null,
	icdprc11 varchar(7) null,
	icdprc12 varchar(7) null,
	allowed	varchar(20) null,
	paid varchar(20) null,
	deductible varchar(20) null,
	copay varchar(20) null,
	coinsamt varchar(20) null,
	cobamt varchar(20) null,
	dispense_fee varchar(20) null,
	bill_id	varchar(27) null,
	bill_spec varchar(8) null,
	rend_id	varchar(27) null,
	rend_spec varchar(8) null,
	prscbr_id varchar(27) null,
	prscbr_spec varchar(8) null,
	ptypeflg varchar(1) null,
	sub_tp_cd varchar(1) null,
	paid_dt	varchar(10) null,
	month_id varchar(6) null
	)
	with(
		appendonly = true,
		orientation = column,
		compresstype = zlib
		)
	distributed by (pat_id);




-- Create iqvia.dx_lookup table:

create table if not exists iqvia.dx_lookup(
	dx_cd varchar(7) null, 
	diagnosis varchar(24) null, 
	diagnosis_desc varchar(1024) null, 
	diag_vers_typ_id varchar(8) null);




-- Create iqvia.pos_lookup table:

create table if not exists iqvia.pos_lookup(
	place_of_svc_cd	varchar(6) null,
	place_of_svc_nm	varchar(225) null,
	place_of_svc_desc	varchar(1024) null);




-- Create iqvia.pr_lookup table:

create table if not exists iqvia.pr_lookup(
	procedure_cd varchar(21) null,
	"procedure" varchar(24) null,
	procedure_desc varchar(2000) null,
	procedure_type_cd varchar(3) null,
	prc_vers_typ_id	varchar(8) null);




-- Create iqvia.rev_lookup table:

create table if not exists iqvia.rev_lookup(
	rev_cd	varchar(12) null, 
	rev_typ_desc varchar(300) null, 
	rev_catg_desc varchar(825) null, 
	rev_subcatg_desc varchar(825) null, 
	rev_short_desc varchar(300) null);




-- Create iqvia.rx_lookup table:

create table if not exists iqvia.rx_lookup(
	ndc	varchar(11) null,
	product_name varchar(105) null,
	generic_name varchar(180) null,
	gpi14 varchar(42) null,
	gpi_desc varchar(180) null,
	gpi2 varchar(6) null,
	gpi2_desc varchar(180) null,
	thptc_clas_id varchar(8) null,
	thptc_clas_desc varchar(180) null,
	dosage_form_nm varchar(240) null,
	route varchar(150) null,
	strength varchar(180) null,
	usc_cd varchar(15) null,
	usc_name varchar(120) null);


