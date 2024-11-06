drop table if exists medicare_national.hha_occurrnce_codes_enc;

create table medicare_national.hha_occurrnce_codes_enc
(
	year text,
	BENE_ID varchar,
	ENC_JOIN_KEY varchar,
	CLM_TYPE_CD varchar,
	RLT_OCRNC_CD_SEQ varchar,
	CLM_RLT_OCRNC_CD varchar,
	CLM_RLT_OCRNC_DT varchar
)
with(
    appendonly = true,
    orientation = column,
    compresstype = zlib
)
distributed by (BENE_ID);