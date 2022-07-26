"""
*******************************************
Copyright: Regione Piemonte 2022
SPDX-License-Identifier: EUPL-1.2-or-later
*******************************************

/***************************************************************************
SentinelAPI
Accesso organizzato a dati Sentinel e generazione automatica di indici di vegetazione 
archiviati su base temporale mensile e trimestrale.
Python scripts, designed for an organization where the Administrators 
want to download Sentinel datasets and calculate vegetation indexes.
Date : 2022-04-16
copyright : (C) 2012-2022 by Regione Piemonte
authors : Luca Guida(Genegis), Fabio Roncato(Trilogis), Stefano Piffer(Trilogis) 
***************************************************************************/

/***************************************************************************
* *
* This program is free software; you can redistribute it and/or modify *
* it under the terms of the GNU General Public License as published by *
* the Free Software Foundation; either version 2 of the License, or *
* (at your option) any later version. *
* *
***************************************************************************/
"""

import datetime
import psycopg2
import sys
from csi_sentinel_config import *

def log_msg(msg):
    print (msg)

def log_title(msg):
    print (Colori.green + msg + Colori.neutro)

def log_warning(msg):
    print (Colori.yellow + msg + Colori.neutro)
    
def log_error(msg):
    print (Colori.red + msg + Colori.neutro)

def printStartScript(nome):
    log_title("\n---------------------------------")
    log_title(" " + str(datetime.datetime.now()))
    log_title(" CSI SENTINEL: " + nome)
    log_title("---------------------------------\n")

def avanzamento(perc, t_restante, riga):
    char_tot = 20
    char_completi = int((char_tot * perc) / 100)
    char_vuoti = char_tot - char_completi
    sys.stdout.write("\r[" + "=" * char_completi +  " " * char_vuoti + "]" +  str(round(perc,2)) + "% - riga: "+ str(riga) + " - stima tempo restante: "+ convertiSecondi(t_restante))
    sys.stdout.flush()

def convertiSecondi(sec):
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    return "%d:%02d:%02d" % (h, m, s)

class Colori:
    # https://en.wikipedia.org/wiki/ANSI_escape_code
    neutro = '\033[00m'    
    red = '\033[91m'
    green = '\033[92m'
    yellow = "\033[93m"
    blue = '\033[94m'
    
class LogDB:
    
    def __init__(self, script_name):
        self.idLog = -1
        self.script_name = script_name
        self.strLog = ""
        self.start_time = datetime.datetime.now()

    def connetti(self):
        self.connLog = psycopg2.connect(dbname=dbname, user=dbuser, host=dbhost, port=dbport, password=dbpass)
        self.curLog = self.connLog.cursor()

    def disconnetti(self):
        self.connLog.commit()
        self.curLog.close()
        self.connLog.close()
        
    def chiudi(self, senzaErrori):
        self.connetti()
        flgStato = 2
        if senzaErrori:
            flgStato = 1
        sql = "UPDATE script_log SET end_time= %s, processing='COMPLETED', flag_state=%s WHERE id = %s"
        sqldata = (datetime.datetime.now(), flgStato, self.idLog)
        self.curLog.execute(sql, sqldata)
        self.disconnetti()
    
    def scrivi(self, processing, message_log):
        self.connetti()
        if message_log != "": 
            self.strLog = self.strLog + "\n" + message_log
        try:
            if self.idLog == -1:
                sql = "INSERT INTO script_log (start_time, processing, message_log, script_name) " \
                      "VALUES (%s, %s, %s, %s) RETURNING id"
                sqldata = (self.start_time, processing, self.strLog, self.script_name)
            else:
                sql = "UPDATE script_log SET "\
                    " processing = %s, "\
                    " message_log = %s "\
                    " WHERE id = %s "
                sqldata = (processing, self.strLog, self.idLog)  
            self.curLog.execute(sql, sqldata)
            if self.idLog == -1:
                self.idLog = self.curLog.fetchone()[0]    
        except Exception as e:
            log_error("Error connection DB: " + str(e))
        self.disconnetti()
        
logDB = LogDB(sys.argv[0])
