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
import psycopg2
import psycopg2.extras
import requests
import os
import pytz
import shutil
from datetime import date, datetime, timedelta
import pytz
from csi_sentinel_config import *
from csi_sentinel_log import *
from csi_sentinel_utils import *

       
######################################################################################### 
        
#########################################################################################
def creaCartella(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
#########################################################################################
        
######################################################################################### 
# delete the zip file present 
#########################################################################################
def deleteOldZipFiles(mainDir, oldDateYear, oldDateMonth):
    if not os.path.exists(directory):
        os.makedirs(directory)
#########################################################################################        

session = requests.Session()
session.auth = (CSI_user, CSI_password)


printStartScript("start procedure to eliminate old zip file already used")
logDB.scrivi("in execution", "")

# open the DB where the information for the download are stored
try:
    conn = psycopg2.connect(dbname=dbname, user=dbuser, host=dbhost, port=dbport, password=dbpass)
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
except:
    print ("ERROR: error in the database connection")

tz = pytz.timezone('Europe/Rome')
# sql = "SELECT uuid_prod, path_download FROM scarichi2 WHERE data_acq < %s AND path_ndvi IS NOT NULL AND path_evi IS NOT NULL AND path_nbr IS NOT NULL AND path_ndwi IS NOT NULL"
#sql = "SELECT uuid_prod, path_images_extracted, path_download FROM scarichi2 WHERE data_acq < %s AND  path_images_extracted IS NOT NULL and data_elaboration_end IS NOT NULL"  
sql = """
    SELECT uuid_prod, path_images_extracted, path_download FROM scarichi2 WHERE data_acq < %s AND path_images_extracted IS NOT NULL AND
    'path_NDVI' IS NOT NULL AND "path_EVI" IS NOT NULL AND "path_NBR" IS NOT NULL AND "path_NDWI" IS NOT NULL AND 
    data_elaboration_end IS NOT NULL AND "path_images_zone" IS NOT NULL AND "path_deltaNBR" IS NOT NULL
    """
sqldata = (datetime.datetime.now(tz) - timedelta(days=delete_zip_older_than_x_days), )
dict_cur.execute(sql, sqldata)


entries = []
for row in dict_cur:
    entries.append(row)
dict_cur.close()
cur = conn.cursor()
folder_deleted = 0


for row in entries:
    uuid = row["uuid_prod"]
    path_images = row["path_images_extracted"] 
    path_zip = row['path_download']
    
    ###############################################
    logDB.scrivi(str(path_images), "")
    shutil.rmtree(str(path_images))
    folder_deleted = folder_deleted+1
    logDB.scrivi("after removed", "")
    print(str(path_images) + " REMOVED! ")
       
    sql = "UPDATE scarichi2 SET path_images_extracted = null WHERE uuid_prod = %s"
    sqldata = (uuid, )
    cur.execute(sql, sqldata) 
    conn.commit()  
    
    ###############################################
    logDB.scrivi(str(path_zip), "")
    os.remove(str(path_zip))
    folder_deleted = folder_deleted+1
    logDB.scrivi("after zip removed", "")
    print(str(path_zip) + " ZIP REMOVED! ")
       
    sql = "UPDATE scarichi2 SET path_download = null WHERE uuid_prod = %s"
    sqldata = (uuid, )
    cur.execute(sql, sqldata) 
    conn.commit()  
     
    
cur.close()
conn.close()

logDB.scrivi(str(folder_deleted) + " folder eliminated", "")
logDB.chiudi(True)
log_msg("procedute for eliminating extracted folder terminated. Eliminated " + str(folder_deleted) + " folder")
