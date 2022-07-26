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
-- TOC entry 814 (class 1259 OID 7387942)
-- Name: script_log; Type: TABLE; Schema: sentinel; Owner: sentinel
--

CREATE TABLE sentinel.script_log (
    id integer NOT NULL,
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    processing character varying(500),
    message_log character varying,
    script_name character varying(100),
    flag_state integer DEFAULT 0 NOT NULL
);


ALTER TABLE sentinel.script_log OWNER TO sentinel;

--
-- TOC entry 7285 (class 0 OID 0)
-- Dependencies: 814
-- Name: COLUMN script_log.flag_state; Type: COMMENT; Schema: sentinel; Owner: sentinel
--

COMMENT ON COLUMN sentinel.script_log.flag_state IS '0: in lavorazione, 1: completato, 2: errore';


--
-- TOC entry 813 (class 1259 OID 7387940)
-- Name: script_log_id_seq; Type: SEQUENCE; Schema: sentinel; Owner: sentinel
--

CREATE SEQUENCE sentinel.script_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE sentinel.script_log_id_seq OWNER TO sentinel;

--
-- TOC entry 7287 (class 0 OID 0)
-- Dependencies: 813
-- Name: script_log_id_seq; Type: SEQUENCE OWNED BY; Schema: sentinel; Owner: sentinel
--

ALTER SEQUENCE sentinel.script_log_id_seq OWNED BY sentinel.script_log.id;


--
-- TOC entry 7064 (class 2604 OID 7387945)
-- Name: script_log id; Type: DEFAULT; Schema: sentinel; Owner: sentinel
--

ALTER TABLE ONLY sentinel.script_log ALTER COLUMN id SET DEFAULT nextval('sentinel.script_log_id_seq'::regclass);


--
-- TOC entry 7286 (class 0 OID 0)
-- Dependencies: 814
-- Name: TABLE script_log; Type: ACL; Schema: sentinel; Owner: sentinel
--

REVOKE ALL ON TABLE sentinel.script_log FROM PUBLIC;
REVOKE ALL ON TABLE sentinel.script_log FROM sentinel;
GRANT ALL ON TABLE sentinel.script_log TO sentinel;
GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE sentinel.script_log TO sentinel_rw;


--
-- TOC entry 7288 (class 0 OID 0)
-- Dependencies: 813
-- Name: SEQUENCE script_log_id_seq; Type: ACL; Schema: sentinel; Owner: sentinel
--

REVOKE ALL ON SEQUENCE sentinel.script_log_id_seq FROM PUBLIC;
REVOKE ALL ON SEQUENCE sentinel.script_log_id_seq FROM sentinel;
GRANT ALL ON SEQUENCE sentinel.script_log_id_seq TO sentinel;
GRANT ALL ON SEQUENCE sentinel.script_log_id_seq TO sentinel_rw;


-- Completed on 2019-11-19 14:00:33

--
-- PostgreSQL database dump complete
--

