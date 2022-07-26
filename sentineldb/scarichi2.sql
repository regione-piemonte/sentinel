-- *******************************************
-- Copyright: Regione Piemonte 2022
-- SPDX-License-Identifier: EUPL-1.2-or-later
-- *******************************************
-- 
-- /***************************************************************************
-- SentinelAPI
-- Accesso organizzato a dati Sentinel e generazione automatica di indici di vegetazione 
-- archiviati su base temporale mensile e trimestrale.
-- Python scripts, designed for an organization where the Administrators 
-- want to download Sentinel datasets and calculate vegetation indexes.
-- Date : 2022-04-16
-- copyright : (C) 2012-2022 by Regione Piemonte
-- authors : Luca Guida(Genegis), Fabio Roncato(Trilogis), Stefano Piffer(Trilogis) 
-- email : XXXX.XXXX@csi.it
-- ***************************************************************************/
-- 
-- /***************************************************************************
-- * *
-- * This program is free software; you can redistribute it and/or modify *
-- * it under the terms of the GNU General Public License as published by *
-- * the Free Software Foundation; either version 2 of the License, or *
-- * (at your option) any later version. *
-- * *
-- ***************************************************************************/

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- TOC entry 1127 (class 1259 OID 7830575)
-- Name: scarichi2; Type: TABLE; Schema: sentinel; Owner: sentinel
--

CREATE TABLE sentinel.scarichi2 (
    uuid_prod character varying(40) NOT NULL,
    title character varying(60),
    link_safe character varying(255),
    icon character varying(255),
    data_acq timestamp without time zone,
    cloud_cvr_p real,
    proc_lev character varying(20),
    size_mb double precision,
    md5 character varying(50),
    path_download character varying(512),
    n_tentativi_download smallint DEFAULT 0,
    data_download timestamp with time zone,
    durata_download smallint,
    path_images_extracted character varying(512),
    data_images_extraction timestamp(6) with time zone,
    data_elaboration_start timestamp(6) with time zone,
    data_elaboration_end timestamp(6) with time zone,
    processing_step character varying(50),
    tile character varying(50),
    doy bigint,
    "path_NDVI" character varying(512),
    "path_EVI" character varying(512),
    "path_NBR" character varying(512),
    "path_NDWI" character varying(512),
    sd_red_channel bigint,
    novalue_red_channel bigint,
    path_images_zone character varying(512)
);


ALTER TABLE sentinel.scarichi2 OWNER TO sentinel;

--
-- TOC entry 7284 (class 0 OID 0)
-- Dependencies: 1127
-- Name: TABLE scarichi2; Type: ACL; Schema: sentinel; Owner: sentinel
--

REVOKE ALL ON TABLE sentinel.scarichi2 FROM PUBLIC;
REVOKE ALL ON TABLE sentinel.scarichi2 FROM sentinel;
GRANT ALL ON TABLE sentinel.scarichi2 TO sentinel;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE sentinel.scarichi2 TO sentinel_rw;


-- Completed on 2019-11-19 13:59:47

--
-- PostgreSQL database dump complete
--

