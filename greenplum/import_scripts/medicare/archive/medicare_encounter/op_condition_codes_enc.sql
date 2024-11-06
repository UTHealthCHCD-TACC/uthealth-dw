drop table if exists medicare_national.op_condition_codes_enc;

create table medicare_national.op_condition_codes_enc
(
	year text,
	BENE_ID varchar,
	ENC_JOIN_KEY varchar,
	CLM_TYPE_CD varchar,
	RLT_COND_CD_SEQ varchar,
	CLM_RLT_COND_CD varchar
)
with(
    appendonly = true,
    orientation = column,
    compresstype = zlib
)
distributed by (BENE_ID);