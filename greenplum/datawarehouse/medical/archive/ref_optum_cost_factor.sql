/* ******************************************************************************************************
 *  Creates and loads reference_tables.ref_optum_cost_factor with static data
 * Run once
 * ******************************************************************************************************
 *  Author || Date      || Notes
 * ******************************************************************************************************
 *  wc001  || 1/01/2021 || script created 
 * ******************************************************************************************************
  *  wallingTACC  || 8/23/2021 || archived
 * *******************************************************************************************************/

CREATE TABLE reference_tables.ref_optum_cost_factor (
	service_type varchar(25) NOT NULL,
	description varchar(150) NOT null,
	standard_price_year integer not null,
	cost_factor double precision not null
)
DISTRIBUTED RANDOMLY;


INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('ANC', 'Ancillary', 2003, 1.437);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('ANC', 'Ancillary', 2004, 1.437);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('ANC', 'Ancillary', 2005, 1.369);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('ANC', 'Ancillary', 2006, 1.325);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('ANC', 'Ancillary', 2007, 1.305);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('ANC', 'Ancillary', 2008, 1.256);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('ANC', 'Ancillary', 2009, 1.221);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('ANC', 'Ancillary', 2010, 1.191);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('ANC', 'Ancillary', 2011, 1.152);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('ANC', 'Ancillary', 2012, 1.125);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('ANC', 'Ancillary', 2013, 1.095);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('ANC', 'Ancillary', 2014, 1.075);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('ANC', 'Ancillary', 2015, 1.058);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('ANC', 'Ancillary', 2016, 1.029);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('ANC', 'Ancillary', 2017, 1.019);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('ANC', 'Ancillary', 2018, 1.011);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('ANC', 'Ancillary', 2019, 1);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_IP', 'Inpatient Hospital Services', 2003, 2.256);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_IP', 'Inpatient Hospital Services', 2004, 2.256);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_IP', 'Inpatient Hospital Services', 2005, 2.089);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_IP', 'Inpatient Hospital Services', 2006, 1.976);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_IP', 'Inpatient Hospital Services', 2007, 1.847);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_IP', 'Inpatient Hospital Services', 2008, 1.737);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_IP', 'Inpatient Hospital Services', 2009, 1.644);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_IP', 'Inpatient Hospital Services', 2010, 1.526);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_IP', 'Inpatient Hospital Services', 2011, 1.398);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_IP', 'Inpatient Hospital Services', 2012, 1.316);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_IP', 'Inpatient Hospital Services', 2013, 1.265);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_IP', 'Inpatient Hospital Services', 2014, 1.182);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_IP', 'Inpatient Hospital Services', 2015, 1.133);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_IP', 'Inpatient Hospital Services', 2016, 1.087);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_IP', 'Inpatient Hospital Services', 2017, 1.045);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_IP', 'Inpatient Hospital Services', 2018, 1.009);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_IP', 'Inpatient Hospital Services', 2019, 1);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_OP', 'Outpatient Hospital Services', 2003, 2.099);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_OP', 'Outpatient Hospital Services', 2004, 2.099);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_OP', 'Outpatient Hospital Services', 2005, 1.943);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_OP', 'Outpatient Hospital Services', 2006, 1.856);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_OP', 'Outpatient Hospital Services', 2007, 1.752);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_OP', 'Outpatient Hospital Services', 2008, 1.632);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_OP', 'Outpatient Hospital Services', 2009, 1.545);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_OP', 'Outpatient Hospital Services', 2010, 1.428);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_OP', 'Outpatient Hospital Services', 2011, 1.359);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_OP', 'Outpatient Hospital Services', 2012, 1.292);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_OP', 'Outpatient Hospital Services', 2013, 1.236);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_OP', 'Outpatient Hospital Services', 2014, 1.172);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_OP', 'Outpatient Hospital Services', 2015, 1.113);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_OP', 'Outpatient Hospital Services', 2016, 1.101);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_OP', 'Outpatient Hospital Services', 2017, 1.042);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_OP', 'Outpatient Hospital Services', 2018, 1.004);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('FAC_OP', 'Outpatient Hospital Services', 2019, 1);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PHARM', 'Prescription Drugs', 2003, 1.591);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PHARM', 'Prescription Drugs', 2004, 1.591);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PHARM', 'Prescription Drugs', 2005, 1.544);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PHARM', 'Prescription Drugs', 2006, 1.492);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PHARM', 'Prescription Drugs', 2007, 1.431);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PHARM', 'Prescription Drugs', 2008, 1.411);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PHARM', 'Prescription Drugs', 2009, 1.39);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PHARM', 'Prescription Drugs', 2010, 1.331);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PHARM', 'Prescription Drugs', 2011, 1.279);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PHARM', 'Prescription Drugs', 2012, 1.227);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PHARM', 'Prescription Drugs', 2013, 1.18);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PHARM', 'Prescription Drugs', 2014, 1.139);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PHARM', 'Prescription Drugs', 2015, 1.082);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PHARM', 'Prescription Drugs', 2016, 1.047);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PHARM', 'Prescription Drugs', 2017, 1.008);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PHARM', 'Prescription Drugs', 2018, 1);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PHARM', 'Prescription Drugs', 2019, 1);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PROF', 'Professional Services', 2003, 1.437);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PROF', 'Professional Services', 2004, 1.437);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PROF', 'Professional Services', 2005, 1.369);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PROF', 'Professional Services', 2006, 1.325);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PROF', 'Professional Services', 2007, 1.305);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PROF', 'Professional Services', 2008, 1.256);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PROF', 'Professional Services', 2009, 1.221);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PROF', 'Professional Services', 2010, 1.191);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PROF', 'Professional Services', 2011, 1.152);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PROF', 'Professional Services', 2012, 1.125);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PROF', 'Professional Services', 2013, 1.095);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PROF', 'Professional Services', 2014, 1.075);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PROF', 'Professional Services', 2015, 1.058);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PROF', 'Professional Services', 2016, 1.029);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PROF', 'Professional Services', 2017, 1.019);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PROF', 'Professional Services', 2018, 1.011);
INSERT INTO reference_tables.ref_optum_cost_factor (service_type, description, standard_price_year, cost_factor) VALUES('PROF', 'Professional Services', 2019, 1);
