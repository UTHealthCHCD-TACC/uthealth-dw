drop table if exists medicare_national.snf_value_codes_enc;

create table medicare_national.snf_value_codes_enc
(
	year text,
	BENE_ID varchar,
	ENC_JOIN_KEY varchar,
	CLM_TYPE_CD varchar,
	RLT_VAL_CD_SEQ varchar,
	CLM_VAL_CD varchar
)
with(
    appendonly = true,
    orientation = column,
    compresstype = zlib
)
distributed by (BENE_ID);