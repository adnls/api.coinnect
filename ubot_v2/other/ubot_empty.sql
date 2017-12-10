-- MySQL dump 10.13  Distrib 5.7.17, for Win64 (x86_64)
--
-- Host: localhost    Database: ubot
-- ------------------------------------------------------
-- Server version	5.7.17-log

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
-- Current Database: `ubot`
--

CREATE DATABASE /*!32312 IF NOT EXISTS*/ `ubot` /*!40100 DEFAULT CHARACTER SET utf8 */;

USE `ubot`;

--
-- Table structure for table `glob`
--

DROP TABLE IF EXISTS `glob`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `glob` (
  `dateheure` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `totalmc` decimal(40,1) DEFAULT NULL,
  `mcvolume24h` decimal(40,1) DEFAULT NULL,
  `btcpct` decimal(10,2) DEFAULT NULL,
  `activecurr` int(10) DEFAULT NULL,
  `activeass` int(10) DEFAULT NULL,
  `activemark` int(10) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `glob_15min`
--

DROP TABLE IF EXISTS `glob_15min`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `glob_15min` (
  `dateheure` datetime NOT NULL,
  `totalmc` decimal(40,1) DEFAULT NULL,
  `mcvolume24h` decimal(40,1) DEFAULT NULL,
  `btcpct` decimal(10,2) DEFAULT NULL,
  `activecurr` int(10) DEFAULT NULL,
  `activeass` int(10) DEFAULT NULL,
  `activemark` int(10) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `glob_1d`
--

DROP TABLE IF EXISTS `glob_1d`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `glob_1d` (
  `dateheure` datetime NOT NULL,
  `totalmc` decimal(40,1) DEFAULT NULL,
  `mcvolume24h` decimal(40,1) DEFAULT NULL,
  `btcpct` decimal(10,2) DEFAULT NULL,
  `activecurr` int(10) DEFAULT NULL,
  `activeass` int(10) DEFAULT NULL,
  `activemark` int(10) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `glob_1h`
--

DROP TABLE IF EXISTS `glob_1h`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `glob_1h` (
  `dateheure` datetime NOT NULL,
  `totalmc` decimal(40,1) DEFAULT NULL,
  `mcvolume24h` decimal(40,1) DEFAULT NULL,
  `btcpct` decimal(10,2) DEFAULT NULL,
  `activecurr` int(10) DEFAULT NULL,
  `activeass` int(10) DEFAULT NULL,
  `activemark` int(10) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `glob_1w`
--

DROP TABLE IF EXISTS `glob_1w`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `glob_1w` (
  `dateheure` datetime NOT NULL,
  `totalmc` decimal(40,1) DEFAULT NULL,
  `mcvolume24h` decimal(40,1) DEFAULT NULL,
  `btcpct` decimal(10,2) DEFAULT NULL,
  `activecurr` int(10) DEFAULT NULL,
  `activeass` int(10) DEFAULT NULL,
  `activemark` int(10) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `glob_2h`
--

DROP TABLE IF EXISTS `glob_2h`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `glob_2h` (
  `dateheure` datetime NOT NULL,
  `totalmc` decimal(40,1) DEFAULT NULL,
  `mcvolume24h` decimal(40,1) DEFAULT NULL,
  `btcpct` decimal(10,2) DEFAULT NULL,
  `activecurr` int(10) DEFAULT NULL,
  `activeass` int(10) DEFAULT NULL,
  `activemark` int(10) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `glob_30min`
--

DROP TABLE IF EXISTS `glob_30min`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `glob_30min` (
  `dateheure` datetime NOT NULL,
  `totalmc` decimal(40,1) DEFAULT NULL,
  `mcvolume24h` decimal(40,1) DEFAULT NULL,
  `btcpct` decimal(10,2) DEFAULT NULL,
  `activecurr` int(10) DEFAULT NULL,
  `activeass` int(10) DEFAULT NULL,
  `activemark` int(10) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `glob_4h`
--

DROP TABLE IF EXISTS `glob_4h`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `glob_4h` (
  `dateheure` datetime NOT NULL,
  `totalmc` decimal(40,1) DEFAULT NULL,
  `mcvolume24h` decimal(40,1) DEFAULT NULL,
  `btcpct` decimal(10,2) DEFAULT NULL,
  `activecurr` int(10) DEFAULT NULL,
  `activeass` int(10) DEFAULT NULL,
  `activemark` int(10) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `glob_5min`
--

DROP TABLE IF EXISTS `glob_5min`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `glob_5min` (
  `dateheure` datetime NOT NULL,
  `totalmc` decimal(40,1) DEFAULT NULL,
  `mcvolume24h` decimal(40,1) DEFAULT NULL,
  `btcpct` decimal(10,2) DEFAULT NULL,
  `activecurr` int(10) DEFAULT NULL,
  `activeass` int(10) DEFAULT NULL,
  `activemark` int(10) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `table_test`
--

DROP TABLE IF EXISTS `table_test`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `table_test` (
  `dateheure` datetime NOT NULL,
  `nom_abreg` varchar(10) DEFAULT NULL,
  `open_usd` decimal(40,20) DEFAULT NULL,
  `close_usd` decimal(40,20) DEFAULT NULL,
  `min_usd` decimal(40,20) DEFAULT NULL,
  `max_usd` decimal(40,20) DEFAULT NULL,
  `open_btc` decimal(40,20) DEFAULT NULL,
  `close_btc` decimal(40,20) DEFAULT NULL,
  `min_btc` decimal(40,20) DEFAULT NULL,
  `max_btc` decimal(40,20) DEFAULT NULL,
  `id` varchar(100) DEFAULT NULL,
  `nom` varchar(100) DEFAULT NULL,
  `volume24h_usd` decimal(40,10) DEFAULT NULL,
  `mc_usd` decimal(30,1) DEFAULT NULL,
  `av_supp` decimal(30,1) DEFAULT NULL,
  `total_supp` decimal(30,1) DEFAULT NULL,
  `change_1h` decimal(10,2) DEFAULT NULL,
  `change_24h` decimal(10,2) DEFAULT NULL,
  `change_7d` decimal(10,2) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `table_test_2`
--

DROP TABLE IF EXISTS `table_test_2`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `table_test_2` (
  `dateheure` datetime NOT NULL,
  `nom_abreg` varchar(10) DEFAULT NULL,
  `open_usd` decimal(40,20) DEFAULT NULL,
  `close_usd` decimal(40,20) DEFAULT NULL,
  `min_usd` decimal(40,20) DEFAULT NULL,
  `max_usd` decimal(40,20) DEFAULT NULL,
  `open_btc` decimal(40,20) DEFAULT NULL,
  `close_btc` decimal(40,20) DEFAULT NULL,
  `min_btc` decimal(40,20) DEFAULT NULL,
  `max_btc` decimal(40,20) DEFAULT NULL,
  `id` varchar(100) DEFAULT NULL,
  `nom` varchar(100) DEFAULT NULL,
  `volume24h_usd` decimal(40,10) DEFAULT NULL,
  `mc_usd` decimal(30,1) DEFAULT NULL,
  `av_supp` decimal(30,1) DEFAULT NULL,
  `total_supp` decimal(30,1) DEFAULT NULL,
  `change_1h` decimal(10,2) DEFAULT NULL,
  `change_24h` decimal(10,2) DEFAULT NULL,
  `change_7d` decimal(10,2) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tick`
--

DROP TABLE IF EXISTS `tick`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tick` (
  `dateheure` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `id` varchar(100) DEFAULT NULL,
  `nom` varchar(100) DEFAULT NULL,
  `nom_abreg` varchar(10) DEFAULT NULL,
  `rang` int(10) DEFAULT NULL,
  `cours_usd` decimal(40,20) DEFAULT NULL,
  `cours_btc` decimal(40,20) DEFAULT NULL,
  `volume24h_usd` decimal(40,10) DEFAULT NULL,
  `mc_usd` decimal(30,1) DEFAULT NULL,
  `av_supp` decimal(30,1) DEFAULT NULL,
  `total_supp` decimal(30,1) DEFAULT NULL,
  `change_1h` decimal(10,2) DEFAULT NULL,
  `change_24h` decimal(10,2) DEFAULT NULL,
  `change_7d` decimal(10,2) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tick_15min`
--

DROP TABLE IF EXISTS `tick_15min`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tick_15min` (
  `dateheure` datetime NOT NULL,
  `nom_abreg` varchar(10) DEFAULT NULL,
  `open_usd` decimal(40,20) DEFAULT NULL,
  `close_usd` decimal(40,20) DEFAULT NULL,
  `min_usd` decimal(40,20) DEFAULT NULL,
  `max_usd` decimal(40,20) DEFAULT NULL,
  `open_btc` decimal(40,20) DEFAULT NULL,
  `close_btc` decimal(40,20) DEFAULT NULL,
  `min_btc` decimal(40,20) DEFAULT NULL,
  `max_btc` decimal(40,20) DEFAULT NULL,
  `id` varchar(100) DEFAULT NULL,
  `nom` varchar(100) DEFAULT NULL,
  `volume24h_usd` decimal(40,10) DEFAULT NULL,
  `mc_usd` decimal(30,1) DEFAULT NULL,
  `av_supp` decimal(30,1) DEFAULT NULL,
  `total_supp` decimal(30,1) DEFAULT NULL,
  `change_1h` decimal(10,2) DEFAULT NULL,
  `change_24h` decimal(10,2) DEFAULT NULL,
  `change_7d` decimal(10,2) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tick_1d`
--

DROP TABLE IF EXISTS `tick_1d`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tick_1d` (
  `dateheure` datetime NOT NULL,
  `nom_abreg` varchar(10) DEFAULT NULL,
  `open_usd` decimal(40,20) DEFAULT NULL,
  `close_usd` decimal(40,20) DEFAULT NULL,
  `min_usd` decimal(40,20) DEFAULT NULL,
  `max_usd` decimal(40,20) DEFAULT NULL,
  `open_btc` decimal(40,20) DEFAULT NULL,
  `close_btc` decimal(40,20) DEFAULT NULL,
  `min_btc` decimal(40,20) DEFAULT NULL,
  `max_btc` decimal(40,20) DEFAULT NULL,
  `id` varchar(100) DEFAULT NULL,
  `nom` varchar(100) DEFAULT NULL,
  `volume24h_usd` decimal(40,10) DEFAULT NULL,
  `mc_usd` decimal(30,1) DEFAULT NULL,
  `av_supp` decimal(30,1) DEFAULT NULL,
  `total_supp` decimal(30,1) DEFAULT NULL,
  `change_1h` decimal(10,2) DEFAULT NULL,
  `change_24h` decimal(10,2) DEFAULT NULL,
  `change_7d` decimal(10,2) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tick_1h`
--

DROP TABLE IF EXISTS `tick_1h`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tick_1h` (
  `dateheure` datetime NOT NULL,
  `nom_abreg` varchar(10) DEFAULT NULL,
  `open_usd` decimal(40,20) DEFAULT NULL,
  `close_usd` decimal(40,20) DEFAULT NULL,
  `min_usd` decimal(40,20) DEFAULT NULL,
  `max_usd` decimal(40,20) DEFAULT NULL,
  `open_btc` decimal(40,20) DEFAULT NULL,
  `close_btc` decimal(40,20) DEFAULT NULL,
  `min_btc` decimal(40,20) DEFAULT NULL,
  `max_btc` decimal(40,20) DEFAULT NULL,
  `id` varchar(100) DEFAULT NULL,
  `nom` varchar(100) DEFAULT NULL,
  `volume24h_usd` decimal(40,10) DEFAULT NULL,
  `mc_usd` decimal(30,1) DEFAULT NULL,
  `av_supp` decimal(30,1) DEFAULT NULL,
  `total_supp` decimal(30,1) DEFAULT NULL,
  `change_1h` decimal(10,2) DEFAULT NULL,
  `change_24h` decimal(10,2) DEFAULT NULL,
  `change_7d` decimal(10,2) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tick_1min`
--

DROP TABLE IF EXISTS `tick_1min`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tick_1min` (
  `dateheure` datetime NOT NULL,
  `nom_abreg` varchar(10) DEFAULT NULL,
  `open_usd` decimal(40,20) DEFAULT NULL,
  `close_usd` decimal(40,20) DEFAULT NULL,
  `min_usd` decimal(40,20) DEFAULT NULL,
  `max_usd` decimal(40,20) DEFAULT NULL,
  `open_btc` decimal(40,20) DEFAULT NULL,
  `close_btc` decimal(40,20) DEFAULT NULL,
  `min_btc` decimal(40,20) DEFAULT NULL,
  `max_btc` decimal(40,20) DEFAULT NULL,
  `id` varchar(100) DEFAULT NULL,
  `nom` varchar(100) DEFAULT NULL,
  `volume24h_usd` decimal(40,10) DEFAULT NULL,
  `mc_usd` decimal(30,1) DEFAULT NULL,
  `av_supp` decimal(30,1) DEFAULT NULL,
  `total_supp` decimal(30,1) DEFAULT NULL,
  `change_1h` decimal(10,2) DEFAULT NULL,
  `change_24h` decimal(10,2) DEFAULT NULL,
  `change_7d` decimal(10,2) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tick_1w`
--

DROP TABLE IF EXISTS `tick_1w`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tick_1w` (
  `dateheure` datetime NOT NULL,
  `nom_abreg` varchar(10) DEFAULT NULL,
  `open_usd` decimal(40,20) DEFAULT NULL,
  `close_usd` decimal(40,20) DEFAULT NULL,
  `min_usd` decimal(40,20) DEFAULT NULL,
  `max_usd` decimal(40,20) DEFAULT NULL,
  `open_btc` decimal(40,20) DEFAULT NULL,
  `close_btc` decimal(40,20) DEFAULT NULL,
  `min_btc` decimal(40,20) DEFAULT NULL,
  `max_btc` decimal(40,20) DEFAULT NULL,
  `id` varchar(100) DEFAULT NULL,
  `nom` varchar(100) DEFAULT NULL,
  `volume24h_usd` decimal(40,10) DEFAULT NULL,
  `mc_usd` decimal(30,1) DEFAULT NULL,
  `av_supp` decimal(30,1) DEFAULT NULL,
  `total_supp` decimal(30,1) DEFAULT NULL,
  `change_1h` decimal(10,2) DEFAULT NULL,
  `change_24h` decimal(10,2) DEFAULT NULL,
  `change_7d` decimal(10,2) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tick_2h`
--

DROP TABLE IF EXISTS `tick_2h`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tick_2h` (
  `dateheure` datetime NOT NULL,
  `nom_abreg` varchar(10) DEFAULT NULL,
  `open_usd` decimal(40,20) DEFAULT NULL,
  `close_usd` decimal(40,20) DEFAULT NULL,
  `min_usd` decimal(40,20) DEFAULT NULL,
  `max_usd` decimal(40,20) DEFAULT NULL,
  `open_btc` decimal(40,20) DEFAULT NULL,
  `close_btc` decimal(40,20) DEFAULT NULL,
  `min_btc` decimal(40,20) DEFAULT NULL,
  `max_btc` decimal(40,20) DEFAULT NULL,
  `id` varchar(100) DEFAULT NULL,
  `nom` varchar(100) DEFAULT NULL,
  `volume24h_usd` decimal(40,10) DEFAULT NULL,
  `mc_usd` decimal(30,1) DEFAULT NULL,
  `av_supp` decimal(30,1) DEFAULT NULL,
  `total_supp` decimal(30,1) DEFAULT NULL,
  `change_1h` decimal(10,2) DEFAULT NULL,
  `change_24h` decimal(10,2) DEFAULT NULL,
  `change_7d` decimal(10,2) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tick_30min`
--

DROP TABLE IF EXISTS `tick_30min`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tick_30min` (
  `dateheure` datetime NOT NULL,
  `nom_abreg` varchar(10) DEFAULT NULL,
  `open_usd` decimal(40,20) DEFAULT NULL,
  `close_usd` decimal(40,20) DEFAULT NULL,
  `min_usd` decimal(40,20) DEFAULT NULL,
  `max_usd` decimal(40,20) DEFAULT NULL,
  `open_btc` decimal(40,20) DEFAULT NULL,
  `close_btc` decimal(40,20) DEFAULT NULL,
  `min_btc` decimal(40,20) DEFAULT NULL,
  `max_btc` decimal(40,20) DEFAULT NULL,
  `id` varchar(100) DEFAULT NULL,
  `nom` varchar(100) DEFAULT NULL,
  `volume24h_usd` decimal(40,10) DEFAULT NULL,
  `mc_usd` decimal(30,1) DEFAULT NULL,
  `av_supp` decimal(30,1) DEFAULT NULL,
  `total_supp` decimal(30,1) DEFAULT NULL,
  `change_1h` decimal(10,2) DEFAULT NULL,
  `change_24h` decimal(10,2) DEFAULT NULL,
  `change_7d` decimal(10,2) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tick_4h`
--

DROP TABLE IF EXISTS `tick_4h`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tick_4h` (
  `dateheure` datetime NOT NULL,
  `nom_abreg` varchar(10) DEFAULT NULL,
  `open_usd` decimal(40,20) DEFAULT NULL,
  `close_usd` decimal(40,20) DEFAULT NULL,
  `min_usd` decimal(40,20) DEFAULT NULL,
  `max_usd` decimal(40,20) DEFAULT NULL,
  `open_btc` decimal(40,20) DEFAULT NULL,
  `close_btc` decimal(40,20) DEFAULT NULL,
  `min_btc` decimal(40,20) DEFAULT NULL,
  `max_btc` decimal(40,20) DEFAULT NULL,
  `id` varchar(100) DEFAULT NULL,
  `nom` varchar(100) DEFAULT NULL,
  `volume24h_usd` decimal(40,10) DEFAULT NULL,
  `mc_usd` decimal(30,1) DEFAULT NULL,
  `av_supp` decimal(30,1) DEFAULT NULL,
  `total_supp` decimal(30,1) DEFAULT NULL,
  `change_1h` decimal(10,2) DEFAULT NULL,
  `change_24h` decimal(10,2) DEFAULT NULL,
  `change_7d` decimal(10,2) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tick_5min`
--

DROP TABLE IF EXISTS `tick_5min`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tick_5min` (
  `dateheure` datetime NOT NULL,
  `nom_abreg` varchar(10) DEFAULT NULL,
  `open_usd` decimal(40,20) DEFAULT NULL,
  `close_usd` decimal(40,20) DEFAULT NULL,
  `min_usd` decimal(40,20) DEFAULT NULL,
  `max_usd` decimal(40,20) DEFAULT NULL,
  `open_btc` decimal(40,20) DEFAULT NULL,
  `close_btc` decimal(40,20) DEFAULT NULL,
  `min_btc` decimal(40,20) DEFAULT NULL,
  `max_btc` decimal(40,20) DEFAULT NULL,
  `id` varchar(100) DEFAULT NULL,
  `nom` varchar(100) DEFAULT NULL,
  `volume24h_usd` decimal(40,10) DEFAULT NULL,
  `mc_usd` decimal(30,1) DEFAULT NULL,
  `av_supp` decimal(30,1) DEFAULT NULL,
  `total_supp` decimal(30,1) DEFAULT NULL,
  `change_1h` decimal(10,2) DEFAULT NULL,
  `change_24h` decimal(10,2) DEFAULT NULL,
  `change_7d` decimal(10,2) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2017-10-03 22:01:53
