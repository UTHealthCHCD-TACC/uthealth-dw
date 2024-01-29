drop table if exists medicare_national.hha_span_codes_enc;

create table medicare_national.hha_span_codes_enc
(
	year text,
	BENE_ID varchar,
	ENC_JOIN_KEY varchar,
	CLM_TYPE_CD varchar,
	RLT_SPAN_CD_SEQ varchar,
	CLM_SPAN_CD varchar,
	CLM_SPAN_FROM_DT varchar,
	CLM_SPAN_THRU_DT varchar
)
with(
    appendonly = true,
    orientation = column,
    compresstype = zlib
)
distributed by (BENE_ID);