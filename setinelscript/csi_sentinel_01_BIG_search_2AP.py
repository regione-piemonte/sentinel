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

import xml.etree.ElementTree
import requests
import sys
import psycopg2
from datetime import date, datetime, timedelta
from csi_sentinel_config import *
from csi_sentinel_log import *

#########################################################################################
# download the MD5 associated with the uuid
def downloadMD5(uuid):
    url = urlMD5 % uuid
    try:
        resp = requests.get(url, auth=(CSI_user, CSI_password), proxies={'http': http_proxy, 'https': https_proxy}, verify=False)
    except requests.exceptions.RequestException as e:
        log_error("Error in the MD5 download:\n" + str(e), 3)
        sys.exit(0)
    return resp.content

#########################################################################################
# parse the date
def _parse_iso_date(content):
    if '.' in content:
        return datetime.datetime.strptime(content, '%Y-%m-%dT%H:%M:%S.%fZ')
    else:
        return datetime.datetime.strptime(content, '%Y-%m-%dT%H:%M:%SZ')
    
#########################################################################################    
# This function receive two datetime object and return the string to happend in the search URL
def create_date_range_string_for_url(from_datetime, to_datetime):
    str_from = from_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    str_to = to_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    return str_from + "%20TO%20" + str_to
  
#########################################################################################
# This function return the days range where the search have imposed.
# The datetime range start from now - days_shift to now (the duration in days is days_shift)    
def get_download_days_range_list(days_shift, days_windows):
    list_range_date = []
    current_datetime = datetime.datetime.now()
    days_before_datetime = current_datetime - timedelta(days=days_shift)    
    while (days_before_datetime < current_datetime):
        list_range_date.append((days_before_datetime, days_before_datetime + timedelta(days=days_windows, hours=1)))
        days_before_datetime = days_before_datetime + timedelta(days=days_windows)
    return list_range_date

#########################################################################################
# use configuration string and replace the '*' symbol in the generic query with the time range have been imposed     
def insert_query_date(query,time_range):
    return query.replace('*', time_range, 1)

#########################################################################################
# use configuration polygon information and replace the '!*' symbol in the generic query with the time range have been imposed 
def insert_query_polygon(query,polygon_range):
    str_polygon_to_add = ""
    for point in polygon_range:
        str_polygon_to_add = str_polygon_to_add + str(point[0]) + '%20' + str(point[1]) + ','
    str_polygon_to_add = str_polygon_to_add[:-1] 
    return query.replace('*!', str_polygon_to_add, 1)
#########################################################################################
    
        
printStartScript("search for available data")
logDB.scrivi("in execution", "")
# connect to the database where the data will be stored
try:
    conn = psycopg2.connect(dbname=dbname, user=dbuser, host=dbhost, port=dbport, password=dbpass)
    cur = conn.cursor()
except:   
    print ("ERROR: error in the database connection")
nEntry = 0    
   
# get a list of the 4 days packed componing the total data range 
range_time_list= get_download_days_range_list(days_before_big_search, days_before)
for rangetime in range_time_list:
    from_datetime, to_datetime = rangetime
    # generate the string to put inside the url for for the API
    date_string_for_url = create_date_range_string_for_url(from_datetime, to_datetime)
    #generate the query with time and polygon setted into the config file
    sentinel_query = insert_query_date(generic_sentinel_query_old, date_string_for_url)   
    query =  insert_query_polygon(sentinel_query, polygon_data_to_download)
   
    url = urlSearchApi + query # create the new query
    log_msg("-----------------------------------")
    log_msg("Query:\n" + query) 
    log_msg("-----------------------------------")      
    try:
        resp = requests.get(url, auth=(CSI_user, CSI_password), proxies={'http': http_proxy, 'https': https_proxy}, verify=False) # call the api and get the returned xml
    except requests.exceptions.RequestException as e:
        log_error("Error in the API call:\n" + str(e))
        sys.exit(0)
    xmlstring = resp.content
        
    # decoding the returned xml response
    feed = xml.etree.ElementTree.fromstring(xmlstring)
    nsAtom = '{http://www.w3.org/2005/Atom}'
    nsOS = '{http://a9.com/-/spec/opensearch/1.1/}'    
    
    # extract the information from the xml string received by the called API entry by entry
    for child in feed:
        if child.tag == nsAtom + 'entry':
            title = ''
            tile = ''
            link = ''
            size = ''
            uuid = ''
            for echild in child:
                if echild.tag == nsAtom + 'title':
                    title = echild.text
                    tile = title[38:44]
                    fmt = '%Y%m%d'
                    s = str(title[11:19])
                    dt = datetime.datetime.strptime(s, fmt)
                    tt = dt.timetuple() 
                    dayoftheyear_images = tt.tm_yday                     
                if echild.tag == nsAtom + 'date':
                    if 'name' in echild.attrib:
                        if echild.attrib['name'] == 'beginposition':
                            beginposition = str(_parse_iso_date(echild.text))
                if echild.tag == nsAtom + 'double':
                    if 'name' in echild.attrib:
                        if echild.attrib['name'] == 'cloudcoverpercentage':
                            cloudcoverpercentage = echild.text
                if echild.tag == nsAtom + 'str':
                    if 'name' in echild.attrib:
                        if echild.attrib['name'] == 'size':
                            size = echild.text.split(" ")
                            size[0] = float(size[0])
                            if size[1] == "GB":
                                size = size[0] * 1024
                            else:
                                size = size[0]
                if echild.tag == nsAtom + 'str':
                    if 'name' in echild.attrib:
                        if echild.attrib['name'] == 'uuid':
                            uuid = echild.text
                if echild.tag == nsAtom + 'str':
                    if 'name' in echild.attrib:
                        if echild.attrib['name'] == 'processinglevel':
                            processinglevel = echild.text
                if echild.tag == nsAtom + 'link':
                    if 'rel' not in echild.attrib:
                        link = echild.attrib['href']
                        #print(link)
                if echild.tag == nsAtom + 'link':
                    if 'rel' in echild.attrib:
                        if echild.attrib["rel"] == "icon":
                            link_icon = echild.attrib['href']
            # get the MD5 of the current file (the current uuid value)   
            md5 = downloadMD5(uuid)
    
            # log the information for the current entry 
            log_msg("\n\n")
            log_title("title: " + title)
            log_msg("link: " + link)
            log_msg("link icon: " + link_icon)
            log_msg("size: " + str(size) + " MB")
            log_msg("uuid: " + uuid)
            log_msg("md5: " + md5)
            log_msg("beginposition: " + beginposition)
            if float(cloudcoverpercentage) < 30.0:
                strcloudcoverpercentage = Colori.green + cloudcoverpercentage + Colori.neutro
            else:
                if float(cloudcoverpercentage) > 50.0:
                    strcloudcoverpercentage = Colori.red + cloudcoverpercentage + Colori.neutro
                else:
                    if float(cloudcoverpercentage) > 30.0:
                        strcloudcoverpercentage = Colori.yellow + cloudcoverpercentage + Colori.neutro
            log_msg("cloud cover percentage: " + strcloudcoverpercentage)
            log_msg("processing level: " + processinglevel)
    
            # increment the number of the entry
            nEntry = nEntry + 1
    
            # add all the information from the entry (if not already inside) into the "scarichi2" table
            sql = "INSERT INTO scarichi2 (uuid_prod, title, link_safe, size_mb, icon, md5, data_acq, cloud_cvr_p, proc_lev, tile, doy, n_tentativi_download) " \
                  "SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0 " \
                  "WHERE '"+uuid+"' NOT IN (SELECT uuid_prod FROM scarichi2)"
            sqldata = (uuid, title, link, size, link_icon, md5, beginposition, cloudcoverpercentage, processinglevel, tile, dayoftheyear_images)
            cur.execute(sql, sqldata)

log_msg("\ntotal returned entry for the search: " + str(nEntry))
conn.commit()
cur.close()
conn.close()
logDB.chiudi(True)
