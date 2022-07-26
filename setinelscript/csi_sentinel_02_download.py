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
import hashlib
import os
from os.path import basename, exists, getsize, join, splitext
from contextlib import closing
import sys
import zipfile
from datetime import date, datetime, timedelta
import pytz
from csi_sentinel_config import *
from csi_sentinel_log import *
from csi_sentinel_utils import *


#########################################################################################
# calculate and return the file MD5 (a void string in case the file do noot exist)
def calculate_md5(fname):
    hash_md5 = hashlib.md5()
    if exists(fname):
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest().upper()
    return ""

#########################################################################################
# file download in chunks
def downloadFile(url, path, session, file_size):
    headers = {}
    downloaded_bytes = 0
    print ("\nenter in download fuction " + str(datetime.datetime.now()))
        
    with closing(session.get(url, stream=True, auth=session.auth, headers=headers, proxies={'http': http_proxy, 'https': https_proxy}, verify=False)) as r:
        chunk_size = 2 ** 20  # download in 1 MB chunks
        mode = 'wb'

        with open(path, mode) as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
                    downloaded_bytes = downloaded_bytes + chunk_size
                    if(downloaded_bytes/(1024*1024.0) > file_size):
                        sys.stdout.write("\rdownload: "+str(file_size)+ " Mb di "+str(file_size)+" Mb" )
                    else:
                        sys.stdout.write("\rdownload: "+str(downloaded_bytes/(1024*1024.0))+ " Mb di "+str(file_size)+" Mb" ) 
                    sys.stdout.flush()

        print ("\ndownload completed !")
        return downloaded_bytes

#########################################################################################
# file icon download                                                                      # DOWNLOAD IMMAGINE ICONA
def downloadFileIcon(url, path, session):                                                # DOWNLOAD IMMAGINE ICONA
    headers = {}                                                                      # DOWNLOAD IMMAGINE ICONA
    downloaded_bytes = 0                                                                      # DOWNLOAD IMMAGINE ICONA

    with closing(session.get(url, stream=True, auth=session.auth, headers=headers)) as r:             # DOWNLOAD IMMAGINE ICONA

        chunk_size = 2 ** 20  # download in 1 MB chunks                                   # DOWNLOAD IMMAGINE ICONA
        mode = 'wb'

        with open(path, mode) as f:                                                                      # DOWNLOAD IMMAGINE ICONA
            for chunk in r.iter_content(chunk_size=chunk_size):                                      # DOWNLOAD IMMAGINE ICONA
                if chunk:  # filter out keep-alive new chunks                            # DOWNLOAD IMMAGINE ICONA
                    f.write(chunk)                                                                      # DOWNLOAD IMMAGINE ICONA
                    downloaded_bytes = downloaded_bytes + chunk_size                         # DOWNLOAD IMMAGINE ICONA              
                    sys.stdout.flush()                                                                      # DOWNLOAD IMMAGINE ICONA

        print ("\ndownload completed !")                                                       # DOWNLOAD IMMAGINE ICONA
        return downloaded_bytes                                                                      # DOWNLOAD IMMAGINE ICONA

        
######################################################################################### 
        
#########################################################################################
def createFolder(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
#########################################################################################
        
     

session = requests.Session()
session.auth = (CSI_user, CSI_password)

printStartScript("start files download")
logDB.scrivi("in execution", "")

# open the DB where the information for the download are stored
try:
    conn = psycopg2.connect(dbname=dbname, user=dbuser, host=dbhost, port=dbport, password=dbpass)
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
except:
    print ("ERROR: error in the database connection")


#sql = "SELECT * FROM scarichi2 WHERE path_images_extracted IS NULL and data_images_extraction IS NULL and link_safe IS NOT NULL ORDER BY data_acq DESC LIMIT " + str(nr_download_block)
sql = "SELECT * FROM scarichi2 WHERE path_images_extracted IS NULL and data_images_extraction IS NULL and link_safe IS NOT NULL and n_tentativi_download < " + str(th_max_tentativi_download) + "ORDER BY data_acq DESC LIMIT " + str(nr_download_block)
dict_cur.execute(sql)


entries = []
for row in dict_cur:
    entries.append(row)
dict_cur.close()
cur = conn.cursor()

for row in entries:
    uuid = row["uuid_prod"]
    link = row["link_safe"]
    link_icon = row["icon"]  # DOWNLOAD IMMAGINE ICONA
    size = row["size_mb"]
    md5_copernicus = row["md5"]
    print (row["title"])
       
    # download file
    anno, mese, scena = splitNomeProdottoSentinel(row["title"])
    nomeFile = row["title"] + '.zip'
    downloadFolder = pathDownloadBase + anno +"/"+ mese +"/"+ scena
    createFolder(downloadFolder)
    zipFileConPath = downloadFolder +"/"+ nomeFile
    log_msg("download "+ zipFileConPath)
    logDB.scrivi("download " + nomeFile, "")  
    # save the current date and time. In case of positive file download this will be the dawnload date stored into the db
    tz = pytz.timezone('Europe/Rome')
    downloadData = datetime.datetime.now(tz)
    # download the file and read the date and time before and after the download to have the time occurred for download
    datetimeStartDownload = datetime.datetime.now(tz)
    downloadFile(link, zipFileConPath, session, size)
    #downloadFileIcon(link_icon, downloadFolder + "/tile.jpg", session)                     # DOWNLOAD IMMAGINE ICONA
    datetimeEndDownload = datetime.datetime.now(tz)
    downloadTime = int((datetimeEndDownload - datetimeStartDownload).total_seconds())
   
    # read the number of download attempt for the file (uuid defines the file)  
    sql = "SELECT n_tentativi_download FROM scarichi2 WHERE uuid_prod = %s"
    sqldata = (uuid, )
    cur.execute(sql, sqldata)
    n_tentativi_download_correnti = cur.fetchall()[0][0]
    # increase the number of download attempt for the file download (the first download attempt the value 0 will became 1)
    n_tentativi_download_correnti = n_tentativi_download_correnti+1
    sql = "UPDATE scarichi2 SET n_tentativi_download = %s WHERE uuid_prod = %s"
    sqldata = (str(n_tentativi_download_correnti), uuid)
    cur.execute(sql, sqldata) 
    conn.commit()     
    
    # veryfy the checksum md5. If it is correct some information will be updated on the table
    # (path_download, data_download, )
    log_msg("verifica checksum")
    logDB.scrivi("verifica checksum " + nomeFile, "")    
    if calculate_md5(zipFileConPath) == md5_copernicus.upper():
        log_msg("checksum md5 corretto")
        # update path_download
        sql = "UPDATE scarichi2 SET path_download = %s WHERE uuid_prod = %s"
        sqldata = (zipFileConPath, uuid)
        cur.execute(sql, sqldata)
        # update data_download        
        sql = "UPDATE scarichi2 SET data_download = %s WHERE uuid_prod = %s"
        sqldata = (downloadData, uuid)
        cur.execute(sql, sqldata)  
        # update durata_download      
        sql = "UPDATE scarichi2 SET durata_download = %s WHERE uuid_prod = %s"
        sqldata = (downloadTime, uuid)
        cur.execute(sql, sqldata)                
        conn.commit()
        
        # unzip file
        log_msg("unzip the file")
        logDB.scrivi("UNZIP " + row["title"], "")
        pathToUnzip = pathUnzipBase + anno +"/"+ mese +"/"+ scena
        createFolder(pathToUnzip)             
        # unzip the downloaded file (only the file on folder "IMG_DATA" will be extracted)
        with zipfile.ZipFile(zipFileConPath) as zip:
            for zip_info in zip.infolist():
                if '/IMG_DATA/' in str(zip_info.filename):
                    zip.extract(zip_info, pathToUnzip) 
                    
                    path_extract_to_store_in_db = str(pathToUnzip + "/" + zip_info.filename)
                    path_extract_to_store_in_db = path_extract_to_store_in_db.split('IMG_DATA', 1)[0] + 'IMG_DATA/'                  
                    # update path_images_extracted
                    sql = "UPDATE scarichi2 SET path_images_extracted = %s WHERE uuid_prod = %s"
                    sqldata = (path_extract_to_store_in_db, uuid)
                    cur.execute(sql, sqldata)
        
                    tz = pytz.timezone('Europe/Rome')
                    extractData = datetime.datetime.now(tz)
                    # update data_images_extraction
                    sql = "UPDATE scarichi2 SET data_images_extraction = %s WHERE uuid_prod = %s"
                    sqldata = (extractData, uuid)
                    cur.execute(sql, sqldata)     
        
        log_msg("file unzipped")
        conn.commit()
    else:               
        logDB.scrivi("error download: " + row["title"], "checksum failed: "+row["title"])
        log_error("ERROR DOWNLOAD: checksum failed ")
        print('calculate_md5: ' + str(calculate_md5(zipFileConPath))) 
        print('md5 from web platform: ' + str(row["md5"]))        
        print('------------------------------------------------')
      

cur.close()
conn.close()

logDB.chiudi(True)
log_msg("download and unzip proceedure concluded")
