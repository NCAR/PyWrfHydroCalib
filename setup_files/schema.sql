-- MySQL dump 10.13  Distrib 5.5.55, for debian-linux-gnu (x86_64)
--
-- Host: localhost    Database: NWM_Calib_DB
-- ------------------------------------------------------
-- Server version	5.5.55-0+deb8u1

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Current Database: `NWM_Calib_DB`
--

CREATE DATABASE /*!32312 IF NOT EXISTS*/ `NWM_Calib_DB` /*!40100 DEFAULT CHARACTER SET latin1 */;

USE `NWM_Calib_DB`;

--
-- Table structure for table `Calib_Params`
--

DROP TABLE IF EXISTS `Calib_Params`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Calib_Params` (
  `jobID` int(11) DEFAULT NULL,
  `domainID` int(11) DEFAULT NULL,
  `iteration` int(11) DEFAULT NULL,
  `paramName` text,
  `paramValue` float DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `Calib_Stats`
--

DROP TABLE IF EXISTS `Calib_Stats`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Calib_Stats` (
  `jobID` int(11) DEFAULT NULL,
  `domainID` int(11) DEFAULT NULL,
  `iteration` int(11) DEFAULT NULL,
  `objfnVal` float DEFAULT NULL,
  `bias` float DEFAULT NULL,
  `rmse` float DEFAULT NULL,
  `cor` float DEFAULT NULL,
  `nse` float DEFAULT NULL,
  `nselog` float DEFAULT NULL,
  `kge` float DEFAULT NULL,
  `fdcerr` float DEFAULT NULL,
  `msof` float DEFAULT NULL,
  `best` int(11) DEFAULT NULL,
  `complete` float DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `Domain_Meta`
--

DROP TABLE IF EXISTS `Domain_Meta`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Domain_Meta` (
  `domainID` int(11) NOT NULL AUTO_INCREMENT,
  `gage_id` text,
  `link_id` int(11) DEFAULT NULL,
  `domain_path` text,
  `gage_agency` text,
  `geo_e` int(11) DEFAULT NULL,
  `geo_w` int(11) DEFAULT NULL,
  `geo_s` int(11) DEFAULT NULL,
  `geo_n` int(11) DEFAULT NULL,
  `hyd_e` int(11) DEFAULT NULL,
  `hyd_w` int(11) DEFAULT NULL,
  `hyd_s` int(11) DEFAULT NULL,
  `hyd_n` int(11) DEFAULT NULL,
  `geo_file` text,
  `wrfinput_file` text,
  `soil_file` text,
  `fulldom_file` text,
  `rtlink_file` text,
  `spweight_file` text,
  `gw_file` text,
  `lake_file` text,
  `forcing_dir` text,
  `obs_file` text,
  `site_name` text,
  `lat` float DEFAULT NULL,
  `lon` float DEFAULT NULL,
  `area_sqmi` float DEFAULT NULL,
  `area_sqkm` float DEFAULT NULL,
  `county_cd` text,
  `state` text,
  `huc2` text,
  `huc4` text,
  `huc6` text,
  `huc8` text,
  `ecol3` text,
  `ecol4` text,
  `rfc` text,
  PRIMARY KEY (`domainID`)
) ENGINE=InnoDB AUTO_INCREMENT=3328 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `Job_Meta`
--

DROP TABLE IF EXISTS `Job_Meta`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Job_Meta` (
  `jobID` int(11) NOT NULL AUTO_INCREMENT,
  `Job_Directory` text,
  `date_su_start` date DEFAULT NULL,
  `date_su_end` date DEFAULT NULL,
  `su_complete` int(11) DEFAULT NULL,
  `date_calib_start` date DEFAULT NULL,
  `date_calib_end` date DEFAULT NULL,
  `date_calib_start_eval` date DEFAULT NULL,
  `num_iter` int(11) DEFAULT NULL,
  `iter_complete` int(11) DEFAULT NULL,
  `calib_complete` int(11) DEFAULT NULL,
  `valid_start_date` date DEFAULT NULL,
  `valid_end_date` date DEFAULT NULL,
  `valid_start_date_eval` date DEFAULT NULL,
  `valid_complete` int(11) DEFAULT NULL,
  `acct_key` text,
  `num_cores_model` int(11) DEFAULT NULL,
  `num_cores_R` int(11) DEFAULT NULL,
  `sql_host` text,
  `job_run_type` int(11) DEFAULT NULL,
  `exe` text,
  `num_gages` int(11) DEFAULT NULL,
  `owner` text,
  `email` text,
  `slack_channel` text,
  `slack_token` text,
  `slack_user` text,
  PRIMARY KEY (`jobID`)
) ENGINE=InnoDB AUTO_INCREMENT=79 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `Job_Params`
--

DROP TABLE IF EXISTS `Job_Params`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Job_Params` (
  `jobID` int(11) DEFAULT NULL,
  `param` text,
  `defaultValue` float DEFAULT NULL,
  `min` float DEFAULT NULL,
  `max` float DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `Valid_Stats`
--

DROP TABLE IF EXISTS `Valid_Stats`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `Valid_Stats` (
  `jobID` int(11) DEFAULT NULL,
  `domainID` int(11) DEFAULT NULL,
  `simulation` text,
  `evalPeriod` text,
  `objfnVal` float DEFAULT NULL,
  `bias` float DEFAULT NULL,
  `rmse` float DEFAULT NULL,
  `cor` float DEFAULT NULL,
  `nse` float DEFAULT NULL,
  `nselog` float DEFAULT NULL,
  `nseWt` float DEFAULT NULL,
  `kge` float DEFAULT NULL,
  `msof` float DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2017-06-27 22:40:28

grant all on NWM_Calib_DB.* to NWM_Calib_rw identified by PWD_TMP;
