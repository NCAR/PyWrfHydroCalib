DROP TABLE IF EXISTS "Calib_Params";
CREATE TABLE "Calib_Params" (
    "jobID" integer DEFAULT NULL,
    "domainID" integer DEFAULT NULL,
    "iteration" integer DEFAULT NULL,
    "paramName" character varying(32),
    "paramValue" real DEFAULT NULL
);
ALTER TABLE "Calib_Params" OWNER TO "WH_Calib_rw";
DROP TABLE IF EXISTS "Calib_Stats";
CREATE TABLE "Calib_Stats" (
   "jobID" integer DEFAULT NULL,
   "domainID" integer DEFAULT NULL,
   "iteration" integer DEFAULT NULL,
   "objfnVal" real DEFAULT NULL,
   "bias" real DEFAULT NULL,
   "rmse" real DEFAULT NULL,
   "cor" real DEFAULT NULL,
   "nse" real DEFAULT NULL,
   "nselog" real DEFAULT NULL,
   "kge" real DEFAULT NULL,
   "fdcerr" real DEFAULT NULL,
   "msof" real DEFAULT NULL,
   "best" integer DEFAULT NULL,
   "complete" float DEFAULT NULL
);
ALTER TABLE "Calib_Stats" OWNER TO "WH_Calib_rw";
DROP TABLE IF EXISTS "Domain_Meta";
CREATE TABLE "Domain_Meta" (
   "domainID" SERIAL PRIMARY KEY,
   "gage_id" character varying(32),
   "link_id" integer DEFAULT NULL,
   "domain_path" character varying(512),
   "gage_agency" character varying(32),
   "geo_e" integer DEFAULT NULL,
   "geo_w" integer DEFAULT NULL,
   "geo_s" integer DEFAULT NULL,
   "geo_n" integer DEFAULT NULL,
   "hyd_e" integer DEFAULT NULL,
   "hyd_w" integer DEFAULT NULL,
   "hyd_s" integer DEFAULT NULL,
   "hyd_n" integer DEFAULT NULL,
   "geo_file" character varying(512),
   "land_spatial_meta_file" character varying(512),
   "wrfinput_file" character varying(512),
   "soil_file" character varying(512),
   "fulldom_file" character varying(512),
   "rtlink_file" character varying(512),
   "spweight_file" character varying(512),
   "gw_file" character varying(512),
   "gw_mask" character varying(512),
   "lake_file" character varying(512),
   "forcing_dir" character varying(512),
   "obs_file" character varying(512),
   "site_name" character varying(512),
   "lat" real DEFAULT NULL,
   "lon" real DEFAULT NULL,
   "area_sqmi" real DEFAULT NULL,
   "area_sqkm" real DEFAULT NULL,
   "county_cd" character varying(32),
   "state" character varying(32),
   "huc2" character varying(64),
   "huc4" character varying(64),
   "huc6" character varying(64),
   "huc8" character varying(64),
   "ecol3" character varying(64),
   "ecol4" character varying(64),
   "rfc" character varying(16),
   "dx_hydro" real DEFAULT NULL,
   "agg_factor" integer DEFAULT NULL,
   "hydro_tbl_spatial" character varying(512)
);
ALTER TABLE "Domain_Meta" OWNER TO "WH_Calib_rw";
DROP TABLE IF EXISTS "Job_Meta";
CREATE TABLE "Job_Meta" (
   "jobID" SERIAL PRIMARY KEY,
   "Job_Directory" character varying(512),
   "date_su_start" timestamp DEFAULT NULL,
   "date_su_end" timestamp DEFAULT NULL,
   "su_complete" integer DEFAULT NULL,
   "date_calib_start" timestamp DEFAULT NULL,
   "date_calib_end" timestamp DEFAULT NULL,
   "date_calib_start_eval" timestamp DEFAULT NULL,
   "num_iter" integer DEFAULT NULL,
   "iter_complete" integer DEFAULT NULL,
   "calib_complete" integer DEFAULT NULL,
   "valid_start_date" timestamp DEFAULT NULL,
   "valid_end_date" timestamp DEFAULT NULL,
   "valid_start_date_eval" timestamp DEFAULT NULL,
   "valid_complete" integer DEFAULT NULL,
   "acct_key" character varying(64),
   "que_name" character varying(64),
   "num_cores_model" integer DEFAULT NULL,
   "num_nodes_model" integer DEFAULT NULL,
   "num_cores_R" integer DEFAULT NULL,
   "num_nodes_R" integer DEFAULT NULL,
   "sql_host" character varying(64),
   "job_run_type" integer DEFAULT NULL,
   "exe" character varying(512),
   "num_gages" integer DEFAULT NULL,
   "owner" character varying(64),
   "email" character varying(64),
   "slack_channel" character varying(64),
   "slack_token" character varying(128),
   "slack_user" character varying(64),
   "analysis_run_type" integer DEFAULT NULL
);
ALTER TABLE "Job_Meta" OWNER TO "WH_Calib_rw";
DROP TABLE IF EXISTS "Job_Params";
CREATE TABLE "Job_Params" (
   "jobID" integer DEFAULT NULL,
   "param" character varying(32),
   "defaultValue" real DEFAULT NULL,
   "min" real DEFAULT NULL,
   "max" real DEFAULT NULL
);
ALTER TABLE "Job_Params" OWNER TO "WH_Calib_rw";
DROP TABLE IF EXISTS "Valid_Stats";
CREATE TABLE "Valid_Stats" (
   "jobID" integer DEFAULT NULL,
   "domainID" integer DEFAULT NULL,
   "simulation" character varying(64),
   "evalPeriod" character varying(64),
   "objfnVal" real DEFAULT NULL,
   "bias" real DEFAULT NULL,
   "rmse" real DEFAULT NULL,
   "cor" real DEFAULT NULL,
   "nse" real DEFAULT NULL,
   "nselog" real DEFAULT NULL,
   "nseWt" real DEFAULT NULL,
   "kge" real DEFAULT NULL,
   "msof" real DEFAULT NULL
);
ALTER TABLE "Valid_Stats" OWNER TO "WH_Calib_rw";
