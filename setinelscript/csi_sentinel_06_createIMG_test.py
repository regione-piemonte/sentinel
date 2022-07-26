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
import sys
sys.path.append('/usr/local/lib64/python2.7/site-packages')
import requests
from osgeo import gdal
import numpy as np
import datetime
import pytz
import os
import osr
import ogr
from datetime import date, datetime, timedelta
from csi_sentinel_config import *
from csi_sentinel_log import *
from csi_sentinel_wcs_wms_template import *

#########################################################################################
def convertToSeconds(sec):
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    return "%d:%02d:%02d" % (h, m, s)
#########################################################################################
                 
#########################################################################################
def createFolder(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
#########################################################################################


t_0 = datetime.datetime.now()
session = requests.Session()
session.auth = (CSI_user, CSI_password)

printStartScript("starting createIMG.....")
logDB.scrivi("in execution", "")

# open the DB where the information for the download are stored
try:
    conn = psycopg2.connect(dbname=dbname, user=dbuser, host=dbhost, port=dbport, password=dbpass)
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
except:
    print ("ERROR: error in the database connection")       

gdal_merge_path  = '/usr/local/bin/gdal_merge.py'
gdalwarp_path  = '/usr/local/bin/gdalwarp'
gdal_translate_path  = '/usr/local/bin/gdal_translate' 
gdal_gdaladdo_path = '/usr/local/bin/gdaladdo'

tz = pytz.timezone('Europe/Rome')
current = datetime.datetime.now(tz)
# extract all the data already elaborated (path of final images is not null) with a date minor to the first day of the current month relative to the reference tile 
# this imply that the results for that months are completed for the specific tile used as referce (setted into the parameters file)
sql = 'SELECT * FROM scarichi2 WHERE data_acq < %s  AND data_elaboration_end IS NOT NULL AND tile = %s AND ("path_NDVI" IS NOT NULL) AND ("path_EVI" IS NOT NULL) AND ("path_NBR" IS NOT NULL) AND ("path_NDWI" IS NOT NULL) AND ("path_images_zone" IS NULL)'
sqldata = (current.replace(minute=59, hour=23, day=1) - timedelta(days=1), referenceTile)
dict_cur.execute(sql, sqldata)
cur = conn.cursor()

entries = []
for row in dict_cur:
    entries.append(row)
print("entries element: " + str(len(entries))) 
# create a set where al the unique element are the tuple available (datetime, tile, year, month)
print("date_tile_month_year_available")
date_tile_month_year_available = set()
for row in entries:
    #date_tile_month_year = (datetime.datetime(int(row["title"][45:49]), int(row["title"][49:51]), 1),str(row["title"][38:44]), str(row["title"][49:51]), str(row["title"][45:49]))
    date_tile_month_year = (datetime.datetime(int(row["title"][11:15]), int(row["title"][15:17]), 1),str(row["title"][38:44]), str(row["title"][15:17]), str(row["title"][11:15]))
    date_tile_month_year_available.add(date_tile_month_year)
# we sort the result. In this manner the most recent available is the one on the top (the first one)
# in this manner the elaboration, in case of a lot of data available will start with the most recent data available
print("date_tile_month_year_available : " + str(date_tile_month_year_available) + "\n")  
####################################################################################    
####################################################################################
#date_tile_month_year_available = sorted(date_tile_month_year_available,reverse = True) #andrebbe False per mettere come primo il piu vecchio
date_tile_month_year_available = sorted(date_tile_month_year_available,reverse = False)
####################################################################################
####################################################################################
print("date_tile_month_year_available sorted: " + str(date_tile_month_year_available) + "\n")    
print("date_tile_month_year_available len: " + str(len(date_tile_month_year_available)))
print("")

# the element have to be into the list of the "first month" of the trimestral images
if(len(date_tile_month_year_available) >= 3):
    ####################################################################################
    ####################################################################################
    #while int(date_tile_month_year_available[0][2]) not in listMonthsAsReference and len(date_tile_month_year_available) is not 0:
    print("(date_tile_month_year_available[len(date_tile_month_year_available)-1][2]): " + str(int(date_tile_month_year_available[len(date_tile_month_year_available)-1][2])))
    while int(date_tile_month_year_available[len(date_tile_month_year_available)-1][2]) not in listMonthsAsReference and len(date_tile_month_year_available) is not 0:
    ####################################################################################
    ####################################################################################        
        ####################################################################################
        ####################################################################################
        #el = date_tile_month_year_available.pop(0) # se abbiamo messo True
        el = date_tile_month_year_available.pop(len(date_tile_month_year_available)-1) # se abbiamo meso False 
        ####################################################################################
        ####################################################################################
        print("one element pop out !! " + str(el))
    

# if the result data are at least three we are looking if the prvious and the previous-previus mont are available
if(len(date_tile_month_year_available) >= 3):  # di 3
    ####################################################################################
    ####################################################################################    
#    data_current = list(date_tile_month_year_available)[0][0]
#    month_current = list(date_tile_month_year_available)[0][2]
#    year_current = list(date_tile_month_year_available)[0][3]
    data_current = list(date_tile_month_year_available)[len(date_tile_month_year_available)-1][0]
    month_current = list(date_tile_month_year_available)[len(date_tile_month_year_available)-1][2]
    year_current = list(date_tile_month_year_available)[len(date_tile_month_year_available)-1][3]    
    ####################################################################################
    ####################################################################################        
    print("data_current " + str(data_current))
    print("month_current " + str(month_current))
    print("year_current " + str(year_current))      
    
    #check if available months before to create the image 
    ####################################################################################
    ####################################################################################          
#    data_one_month_before = ((list(date_tile_month_year_available)[0][0]).replace(day=1) - timedelta(days=1)).replace(day=1)
    data_one_month_before = ((list(date_tile_month_year_available)[len(date_tile_month_year_available)-1][0]).replace(day=1) - timedelta(days=1)).replace(day=1)
    ####################################################################################
    ####################################################################################         
    month_one_month_before = data_one_month_before.month
    year_one_month_before = data_one_month_before.year
    tupleToVerifyOneMonthBefore = (data_one_month_before, referenceTile, str(month_one_month_before).zfill(2), str(year_one_month_before))
    print(str(tupleToVerifyOneMonthBefore))
    print(str("data_one_month_before " + str(data_one_month_before)))
    print(str("month_one_month_before " + str(month_one_month_before)))
    print(str("year_one_month_before " + str(year_one_month_before)))
    
    data_two_month_before = (data_one_month_before - timedelta(days=1)).replace(day=1)
    month_two_month_before = data_two_month_before.month
    year_two_month_before = data_two_month_before.year
    tupleToVerifyTwoMonthBefore = (data_two_month_before, referenceTile, str(month_two_month_before).zfill(2), str(year_two_month_before))
    print(str(tupleToVerifyTwoMonthBefore))
    print(str("data_two_month_before " + str(data_two_month_before)))
    print(str("month_two_month_before " + str(month_two_month_before)))
    print(str("year_two_month_before " + str(year_two_month_before)))
    print("")
    
    # now we will verify if the three months data are available. If yes we can go further. For the current month are sure available. We will look
    # for the month before and for the one before before
    if(tupleToVerifyOneMonthBefore in date_tile_month_year_available and tupleToVerifyTwoMonthBefore in date_tile_month_year_available and int(month_current) in listMonthsAsReference):
        print("The three month images are available for creating RGB tri-month image")     
        #search the datetime to use as refernce datetime. We are the three month available for the reference tile. We now will look inside this months and
        # for the used reference tile we are looking for the best data_acq where take and use the image of the reference tile. Consequently we will use this data to look
        # for the others data of the tiles will compose the global visible image (mosaic of all the tiles available)            
        sql = """
            SELECT * FROM scarichi2 WHERE (EXTRACT(MONTH FROM data_acq) = %s AND EXTRACT(YEAR FROM data_acq) = %s OR
            EXTRACT(MONTH FROM data_acq) = %s AND EXTRACT(YEAR FROM data_acq) = %s OR
            EXTRACT(MONTH FROM data_acq) = %s AND EXTRACT(YEAR FROM data_acq) = %s) AND
            'path_NDVI' IS NOT NULL AND "path_EVI" IS NOT NULL AND "path_NBR" IS NOT NULL AND "path_NDWI" IS NOT NULL AND 
            "path_images_zone" IS NULL AND tile = %s AND novalue_red_channel < %s AND sd_red_channel > %s ORDER BY cloud_cvr_p ASC LIMIT 1;
            """            
        sqldata = (month_current, year_current, month_one_month_before, year_one_month_before, month_two_month_before, year_two_month_before, referenceTile, th_novalue_red_channel,th_sd_red_channel)
        
        dict_cur.execute(sql, sqldata)
        row = dict_cur.fetchone()
        reference_timestamp = row['data_acq']
        print("The reference time is " + str(reference_timestamp))
        # now here we will have a  timestamp,  timestamp for the reference tile. This will be the starting point for the others data-tile couple
                                                                                                
        # select for each tile the candidate that respect a series of propeties. At the end of this query we have the list for all the tiles of which folder
        # contain the images to will be taken for create the trimonth image
              
        sql = """
            SELECT * FROM scarichi2 WHERE (tile,cloud_cvr_p) IN 
            ( SELECT (tile), MIN(cloud_cvr_p)
            FROM scarichi2
            WHERE  novalue_red_channel < %s AND sd_red_channel > %s AND             
            (EXTRACT(MONTH FROM data_acq) = %s AND EXTRACT(YEAR FROM data_acq) = %s OR
            EXTRACT(MONTH FROM data_acq) = %s AND EXTRACT(YEAR FROM data_acq) = %s OR
            EXTRACT(MONTH FROM data_acq) = %s AND EXTRACT(YEAR FROM data_acq) = %s) AND           
            'path_NDVI' IS NOT NULL AND
            data_acq BETWEEN %s - interval '%s day' AND %s + interval '%s day'
            GROUP BY tile);
            """        
        sqldata = (th_novalue_red_channel, th_sd_red_channel, month_current, year_current, month_one_month_before, year_one_month_before, month_two_month_before, year_two_month_before, reference_timestamp, th_days_around_reference_day, reference_timestamp,th_days_around_reference_day)
        #print(sqldata)
        dict_cur.execute(sql, sqldata)                
        entries = []
        for row in dict_cur:
            entries.append(row)        
        cur = conn.cursor()
        filesAvailable_TC_4_3_2 = []
        filesAvailable_SC_8_4_3 = []
        filesAvailable_SC_8_11_4 = []
        for row in entries:
            uuid = row["uuid_prod"]
            tile = row["tile"]
            data_acq = row["data_acq"]
            cloud_cvr_p = row["cloud_cvr_p"]
            path_images_extracted = row["path_images_extracted"]
            print("")
            print(tile + " - " + str(data_acq) + " - " +  str(cloud_cvr_p) + " - path: " + str(path_images_extracted))  
            if(path_images_extracted != None):
                outfile_4_3_2 = path_images_extracted + "R10m/" + tile + "_" + data_acq.strftime("%Y%m%dT%H%M%S") + "_TC_4-3-2_10m.tif"   
                outfile_8_4_3 = path_images_extracted + "R10m/" + tile + "_" + data_acq.strftime("%Y%m%dT%H%M%S") + "_SC_8-4-3_10m.tif" 
                outfile_8_11_4 = path_images_extracted + "R10m/" + tile + "_" + data_acq.strftime("%Y%m%dT%H%M%S") + "_SC_8-11-4_10m.tif"  
                
                infileBand4 = path_images_extracted + "R10m/" + tile + "_" + data_acq.strftime("%Y%m%dT%H%M%S") + "_B04_10m.jp2"
                if not os.path.isfile(infileBand4):
                    infileBand4 = path_images_extracted + "R10m/L2A_" + tile + "_" + data_acq.strftime("%Y%m%dT%H%M%S") + "_B04_10m.jp2"
                    
                infileBand3 = path_images_extracted + "R10m/" + tile + "_" + data_acq.strftime("%Y%m%dT%H%M%S") + "_B03_10m.jp2"
                if not os.path.isfile(infileBand3):
                   infileBand3 = path_images_extracted + "R10m/L2A_" + tile + "_" + data_acq.strftime("%Y%m%dT%H%M%S") + "_B03_10m.jp2" 
                   
                infileBand2 = path_images_extracted + "R10m/" + tile + "_" + data_acq.strftime("%Y%m%dT%H%M%S") + "_B02_10m.jp2" 
                if not os.path.isfile(infileBand2):
                    infileBand2 = path_images_extracted + "R10m/L2A_" + tile + "_" + data_acq.strftime("%Y%m%dT%H%M%S") + "_B02_10m.jp2"
                    
                infileBand8 = path_images_extracted + "R10m/" + tile + "_" + data_acq.strftime("%Y%m%dT%H%M%S") + "_B08_10m.jp2"
                if not os.path.isfile(infileBand8):
                    infileBand8 = path_images_extracted + "R10m/L2A_" + tile + "_" + data_acq.strftime("%Y%m%dT%H%M%S") + "_B08_10m.jp2"
                    
                infileBand11 = path_images_extracted + "R10m/" + tile + "_" + data_acq.strftime("%Y%m%dT%H%M%S") + "_B11_10m_derived.jp2"
                if not os.path.isfile(infileBand11):
                    infileBand11 = path_images_extracted + "R10m/L2A_" + tile + "_" + data_acq.strftime("%Y%m%dT%H%M%S") + "_B11_10m_derived.jp2"
                
                print("Generate TC-4-3-2 image for the current tile.....................................") # https://gis.stackexchange.com/questions/233826/get-rgb-images-from-sentinel-2-using-gdal
                gdal_merge_str = '{0} {1} {2} {3} {4} {5} {6} {7} {8}' 
                gdal_merge_process = gdal_merge_str.format(gdal_merge_path, "-separate", "-co", "PHOTOMETRIC=RGB", "-o", outfile_4_3_2, infileBand4, infileBand3, infileBand2)
                print(gdal_merge_process)
                ret = os.system(gdal_merge_process)    
                print("return value " + str(ret))   
                print(" -------------------------------------------------------------------------------- ")
                
                print("Generate SC-8-4-3 image for the current tile.....................................") # https://gis.stackexchange.com/questions/233826/get-rgb-images-from-sentinel-2-using-gdal
                gdal_merge_str = '{0} {1} {2} {3} {4} {5} {6} {7} {8}' 
                gdal_merge_process = gdal_merge_str.format(gdal_merge_path, "-separate", "-co", "PHOTOMETRIC=RGB", "-o", outfile_8_4_3, infileBand8, infileBand4, infileBand3)
                print(gdal_merge_process)
                ret = os.system(gdal_merge_process)    
                print("return value " + str(ret))   
                print(" -------------------------------------------------------------------------------- ")

                print("Generate SC-8-11-4 image for the current tile.....................................") # https://gis.stackexchange.com/questions/233826/get-rgb-images-from-sentinel-2-using-gdal
                gdal_merge_str = '{0} {1} {2} {3} {4} {5} {6} {7} {8}' 
                gdal_merge_process = gdal_merge_str.format(gdal_merge_path, "-separate", "-co", "PHOTOMETRIC=RGB", "-o", outfile_8_11_4, infileBand8, infileBand11, infileBand4)
                print(gdal_merge_process)
                ret = os.system(gdal_merge_process)    
                print("return value " + str(ret))   
                print(" -------------------------------------------------------------------------------- ")
                
                folderDestination = pathSaveTriMonthImageBase +"/"+ year_current +"/"+ str(month_current) + "-" + str(month_one_month_before) + "-" + str(month_two_month_before)
                ################################################# TMP TMP #################################################
                # se non trovo la cartella temporanea del trimestre, prendo la cartella del trimestre corretta (se disponibile) e la faccio diventare la temporanea
                # in questo modo mantengo i vecchi calcoli e posso fare tutto nuovamente. Cosi fancendo trasferisco solo i dati prima volta che faccio tutto e non ho
                # gia effettuato backup dei dati gia presenti.
                
                # non esiste qui variabile anno(year)
                #datetime_threshold = datetime.datetime(year_before_images_available, month_before_images_available, 1)
                #datetime_data = datetime.datetime(int(year), int(month),1)
                #if(datetime_data<datetime_threshold):        
                folderIndicatorMonthOldBackup = pathSaveTriMonthImageBase +"/"+ year_current +"/"+ str(month_current) + "-" + str(month_one_month_before) + "-" + str(month_two_month_before) + "_tmp"
                folderIndicatorMonth = pathSaveTriMonthImageBase +"/"+ year_current +"/"+ str(month_current) + "-" + str(month_one_month_before) + "-" + str(month_two_month_before)
                if not os.path.exists(folderIndicatorMonthOldBackup):
                    if os.path.exists(folderIndicatorMonth):
                        os.rename(folderIndicatorMonth, folderIndicatorMonthOldBackup)
                ################################################# TMP TMP #################################################                             
                createFolder(folderDestination)    
                fileDestination_outfile_4_3_2 = folderDestination + "/TC_4-3-2_" + tile + "_" + data_acq.strftime("%Y%m%d") + ".tif"
                print("fileDestination_outfile_4_3_2: " + fileDestination_outfile_4_3_2) 
                os.rename(outfile_4_3_2, fileDestination_outfile_4_3_2)        
                filesAvailable_TC_4_3_2.append(fileDestination_outfile_4_3_2) 
                
                fileDestination_outfile_8_4_3 = folderDestination + "/SC_8-4-3_" + tile + "_" + data_acq.strftime("%Y%m%d") + ".tif"                              
                print("fileDestination_outfile_8_4_3: " + fileDestination_outfile_8_4_3)
                os.rename(outfile_8_4_3, fileDestination_outfile_8_4_3)        
                filesAvailable_SC_8_4_3.append(fileDestination_outfile_8_4_3) 
                
                fileDestination_outfile_8_11_4 = folderDestination + "/SC_8-11-4_" + tile + "_" + data_acq.strftime("%Y%m%d") + ".tif" 
                print("fileDestination_outfile_8_11_4: " + fileDestination_outfile_8_11_4)
                os.rename(outfile_8_11_4, fileDestination_outfile_8_11_4)        
                filesAvailable_SC_8_11_4.append(fileDestination_outfile_8_11_4)                    
            else:                
                print("Images not available. Maybe it was deleted before the creation of RGB image")
                message_images_not_available = "ERROR: source images not available"
                sql = "UPDATE scarichi2 SET path_images_zone = %s WHERE uuid_prod = %s"
                sqldata = (message_images_not_available, uuid)                                                  
                dict_cur.execute(sql, sqldata)  
                conn.commit()
                print(sqldata)
                
        print("")
        if(len(filesAvailable_TC_4_3_2) != 0 and len(filesAvailable_SC_8_4_3) != 0 and len(filesAvailable_SC_8_11_4) != 0): # at least an image is available
            ####################################################################
            #  TC 4-3-2
            ####################################################################   
            print("Generate TC_4-3-2 image for all the tiles.....................................") # https://gis.stackexchange.com/questions/230553/merging-all-tiles-from-one-directory-using-gdal/230588
            fileDestination_TC_4_3_2 = folderDestination + "/mosaic_TC_4-3-2_" + year_current + "_" + str(month_current) + "-" + str(month_one_month_before) + "-" + str(month_two_month_before) + ".tif"    
            fileDestination_TC_4_3_2_tmp0 = folderDestination + "/mosaic_TC_4-3-2_" + year_current + "_" + str(month_current) + "-" + str(month_one_month_before) + "-" + str(month_two_month_before) + "_tmp0.tif"    
            fileDestination_TC_4_3_2_tmp1 = folderDestination + "/mosaic_TC_4-3-2_" + year_current + "_" + str(month_current) + "-" + str(month_one_month_before) + "-" + str(month_two_month_before) + "_tmp1.tif"    
            gdal_warp_str = '{0}' 
            gdal_warp_process = gdal_warp_str.format(gdalwarp_path)        
            for fileAvailable in filesAvailable_TC_4_3_2:
                gdal_warp_process = gdal_warp_process + " " + str(fileAvailable)
            gdal_warp_process = gdal_warp_process + " " + str(fileDestination_TC_4_3_2_tmp0)     
            print(gdal_warp_process) 
            ret = os.system(gdal_warp_process)    
            print("return value " + str(ret))

            print("Initial compress image TC_4_3_2")
            gdal_calc_str = '{0} -of {1} -co {2} -co {3} -co {4} {5} {6}'
            gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff" , "COMPRESS=LZW", "TILED=YES", "BIGTIFF=YES", fileDestination_TC_4_3_2_tmp0, fileDestination_TC_4_3_2_tmp1)
            print(gdal_calc_process)
            os.system(gdal_calc_process)                
            print("Adding overview levels TC_4_3_2")
            gdal_calc_str = '{0} -r average {1} 2 4 8 16 32 64 --config {2} --config {3}'
            gdal_calc_process = gdal_calc_str.format(gdal_gdaladdo_path, fileDestination_TC_4_3_2_tmp1 , "COMPRESS_OVERVIEW JPEG", "INTERLEAVE_OVERVIEW PIXEL")
            print(gdal_calc_process)
            os.system(gdal_calc_process)            
            print("Compress and move image TC_4_3_2")
            gdal_calc_str = '{0} -of {1} -co {2} -co {3} -co {4} -co {5} -co {6} {7} {8}'
            gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff", "COPY_SRC_OVERVIEWS=YES" , "COMPRESS=LZW", "PREDICTOR=2", "TILED=YES", "BIGTIFF=YES", fileDestination_TC_4_3_2_tmp1, fileDestination_TC_4_3_2)
            print(gdal_calc_process)
            os.system(gdal_calc_process)  
            os.remove(fileDestination_TC_4_3_2_tmp0)
            os.remove(fileDestination_TC_4_3_2_tmp1) 
            print(fileDestination_TC_4_3_2)
            
            for fileAvailable in filesAvailable_TC_4_3_2:
                os.remove(str(fileAvailable))
            

            ################################################# TMP TMP #################################################
            month_one_month_before = data_one_month_before.month
            year_one_month_before = data_one_month_before.year
            datetime_threshold = datetime.datetime(year_before_inc_file_already_available, month_before_inc_file_already_available, 1)
            datetime_data = datetime.datetime(int(year_one_month_before), int(month_one_month_before),1)
            if(datetime_data>=datetime_threshold): # questo per evitare di andare a ricreare nuovamente se vado a rielaborare dati gia fatti !!!!!!!!
            ###if(year>=year_before_inc_file_already_available and month >= month_before_inc_file_already_available): # questo per evitare di andare a ricreare nuovamente se vado a rielaborare dati gia fatti !!!!!!!!
            ################################################# TMP TMP #################################################    
                ####################################################################
                # generate file .inc and fill its for TC_4_3_2
                ####################################################################               
                createFolder(include_ogc_path + '/TC_4-3-2/' + str(year_current))
                TC_4_3_2_wms = open(include_ogc_path + '/TC_4-3-2/' + str(year_current) + "/wms_" + str(month_current).zfill(2)  + "-" + str(month_one_month_before).zfill(2) + "-" + str(month_two_month_before).zfill(2) + ".inc" , 'w')        
                TC_4_3_2_wms_template = template_wms        
                TC_4_3_2_wms_template = TC_4_3_2_wms_template.replace("$INDICE$","TC_4-3-2")
                TC_4_3_2_wms_template = TC_4_3_2_wms_template.replace("$ANNO$",str(year_current))
                TC_4_3_2_wms_template = TC_4_3_2_wms_template.replace("$MESE$",str(month_current) + "-" + str(month_one_month_before) + "-" + str(month_two_month_before))
                TC_4_3_2_wms_template = TC_4_3_2_wms_template.replace("$VRT_PATH$", fileDestination_TC_4_3_2)  
                TC_4_3_2_wms_template = TC_4_3_2_wms_template.replace("$BANDS$","1,2,3")
                TC_4_3_2_wms_template = TC_4_3_2_wms_template.replace("$PROCESSING$","SCALE=0,3000")#"SCALE=AUTO")
                TC_4_3_2_wms.write(TC_4_3_2_wms_template) 
                TC_4_3_2_wms.close()
                mapfile_wms_TC_4_3_2_file = open(static_mapfile_wms_TC_4_3_2 , 'a')  
                mapfile_wms_TC_4_3_2_string = 'INCLUDE "' + str(include_ogc_path) + "/TC_4-3-2/" + str(year_current) + "/wms_"+ str(month_current).zfill(2)  + "-" + str(month_one_month_before).zfill(2) + "-" + str(month_two_month_before).zfill(2) + '.inc"\n' 
                mapfile_wms_TC_4_3_2_file.write(mapfile_wms_TC_4_3_2_string)
                mapfile_wms_TC_4_3_2_file.close()       
                
                TC_4_3_2_wcs = open(include_ogc_path + '/TC_4-3-2/' + str(year_current) + "/wcs_" + str(month_current).zfill(2)  + "-" + str(month_one_month_before).zfill(2) + "-" + str(month_two_month_before).zfill(2) + ".inc" , 'w')
                TC_4_3_2_wcs_template = template_wcs_multibans3        
                TC_4_3_2_wcs_template = TC_4_3_2_wcs_template.replace("$INDICE$","TC_4-3-2")
                TC_4_3_2_wcs_template = TC_4_3_2_wcs_template.replace("$ANNO$",str(year_current))
                TC_4_3_2_wcs_template = TC_4_3_2_wcs_template.replace("$MESE$",str(month_current) + "-" + str(month_one_month_before) + "-" + str(month_two_month_before))
                TC_4_3_2_wcs_template = TC_4_3_2_wcs_template.replace("$VRT_PATH$", fileDestination_TC_4_3_2)                
                TC_4_3_2_wcs.write(TC_4_3_2_wcs_template)         
                TC_4_3_2_wcs.close()
                mapfile_wcs_TC_4_3_2_file = open(static_mapfile_wcs_TC_4_3_2 , 'a')  
                mapfile_wcs_TC_4_3_2_string = 'INCLUDE "' + str(include_ogc_path) + "/TC_4-3-2/" + str(year_current) + "/wcs_" + str(month_current).zfill(2)  + "-" + str(month_one_month_before).zfill(2) + "-" + str(month_two_month_before).zfill(2) + '.inc"\n' 
                mapfile_wcs_TC_4_3_2_file.write(mapfile_wcs_TC_4_3_2_string)
                mapfile_wcs_TC_4_3_2_file.close()   
                            
            
            ####################################################################
            #  TC 8-4-3
            ####################################################################            
            print("Generate SC_8-4-3 image for all the tiles.....................................") # https://gis.stackexchange.com/questions/230553/merging-all-tiles-from-one-directory-using-gdal/230588
            fileDestination_SC_8_4_3 = folderDestination + "/mosaic_SC_8-4-3_" + year_current + "_" + str(month_current) + "-" + str(month_one_month_before) + "-" + str(month_two_month_before) + ".tif"    
            fileDestination_SC_8_4_3_tmp0 = folderDestination + "/mosaic_SC_8-4-3_" + year_current + "_" + str(month_current) + "-" + str(month_one_month_before) + "-" + str(month_two_month_before) + "_tmp0.tif"    
            fileDestination_SC_8_4_3_tmp1 = folderDestination + "/mosaic_SC_8-4-3_" + year_current + "_" + str(month_current) + "-" + str(month_one_month_before) + "-" + str(month_two_month_before) + "_tmp1.tif"                
            
            gdal_warp_str = '{0}' 
            gdal_warp_process = gdal_warp_str.format(gdalwarp_path)        
            for fileAvailable in filesAvailable_SC_8_4_3:
                gdal_warp_process = gdal_warp_process + " " + str(fileAvailable)
            gdal_warp_process = gdal_warp_process + " " + str(fileDestination_SC_8_4_3_tmp0)     
            print(gdal_warp_process) 
            ret = os.system(gdal_warp_process)    
            print("return value " + str(ret))
            
            print("Initial compress image SC_8_4_3")
            gdal_calc_str = '{0} -of {1} -co {2} -co {3} -co {4} {5} {6}'
            gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff" , "COMPRESS=LZW", "TILED=YES", "BIGTIFF=YES", fileDestination_SC_8_4_3_tmp0, fileDestination_SC_8_4_3_tmp1)
            print(gdal_calc_process)
            os.system(gdal_calc_process)                
            print("Adding overview levels SC_8_4_3")
            gdal_calc_str = '{0} -r average {1} 2 4 8 16 32 64 --config {2} --config {3}'
            gdal_calc_process = gdal_calc_str.format(gdal_gdaladdo_path, fileDestination_SC_8_4_3_tmp1 , "COMPRESS_OVERVIEW JPEG", "INTERLEAVE_OVERVIEW PIXEL")
            print(gdal_calc_process)
            os.system(gdal_calc_process)            
            print("Compress and move image SC_8_4_3")
            gdal_calc_str = '{0} -of {1} -co {2} -co {3} -co {4} -co {5} -co {6} {7} {8}'
            gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff", "COPY_SRC_OVERVIEWS=YES" , "COMPRESS=LZW", "PREDICTOR=2", "TILED=YES", "BIGTIFF=YES", fileDestination_SC_8_4_3_tmp1, fileDestination_SC_8_4_3)
            print(gdal_calc_process) 
            os.system(gdal_calc_process)
            os.remove(fileDestination_SC_8_4_3_tmp0)
            os.remove(fileDestination_SC_8_4_3_tmp1) 
            print(fileDestination_SC_8_4_3)
            
            for fileAvailable in filesAvailable_SC_8_4_3:
                os.remove(str(fileAvailable))   
    
    
            ################################################# TMP TMP #################################################
            month_one_month_before = data_one_month_before.month
            year_one_month_before = data_one_month_before.year            
            datetime_threshold = datetime.datetime(year_before_inc_file_already_available, month_before_inc_file_already_available, 1)
            datetime_data = datetime.datetime(int(year_one_month_before), int(month_one_month_before),1)
            if(datetime_data>=datetime_threshold): # questo per evitare di andare a ricreare nuovamente se vado a rielaborare dati gia fatti !!!!!!!!
            ###if(year>=year_before_inc_file_already_available and month >= month_before_inc_file_already_available): # questo per evitare di andare a ricreare nuovamente se vado a rielaborare dati gia fatti !!!!!!!!
            ################################################# TMP TMP #################################################    
                ####################################################################
                # generate file .inc and fill its for SC_8_4_3
                ####################################################################      
                createFolder(include_ogc_path + '/SC_8-4-3/' + str(year_current))
                SC_8_4_3_wms = open(include_ogc_path + '/SC_8-4-3/' + str(year_current) + "/wms_" + str(month_current).zfill(2)  + "-" + str(month_one_month_before).zfill(2) + "-" + str(month_two_month_before).zfill(2) + ".inc" , 'w')        
                SC_8_4_3_wms_template = template_wms        
                SC_8_4_3_wms_template = SC_8_4_3_wms_template.replace("$INDICE$","SC_8-4-3")
                SC_8_4_3_wms_template = SC_8_4_3_wms_template.replace("$ANNO$",str(year_current))
                SC_8_4_3_wms_template = SC_8_4_3_wms_template.replace("$MESE$",str(month_current) + "-" + str(month_one_month_before) + "-" + str(month_two_month_before))
                SC_8_4_3_wms_template = SC_8_4_3_wms_template.replace("$VRT_PATH$", fileDestination_SC_8_4_3)
                SC_8_4_3_wms_template = SC_8_4_3_wms_template.replace("$BANDS$","1,2,3")
                SC_8_4_3_wms_template = SC_8_4_3_wms_template.replace("$PROCESSING$","SCALE=0,5000")#"SCALE=AUTO")
                SC_8_4_3_wms.write(SC_8_4_3_wms_template) 
                SC_8_4_3_wms.close()
                mapfile_wms_SC_8_4_3_file = open(static_mapfile_wms_SC_8_4_3 , 'a')  
                mapfile_wms_SC_8_4_3_string = 'INCLUDE "' + str(include_ogc_path) + "/SC_8-4-3/" + str(year_current) + "/wms_" + str(month_current).zfill(2)  + "-" + str(month_one_month_before).zfill(2) + "-" + str(month_two_month_before).zfill(2) + '.inc"\n' 
                mapfile_wms_SC_8_4_3_file.write(mapfile_wms_SC_8_4_3_string)
                mapfile_wms_SC_8_4_3_file.close()       
                
                SC_8_4_3_wcs = open(include_ogc_path + '/SC_8-4-3/' + str(year_current) + "/wcs_" + str(month_current).zfill(2)  + "-" + str(month_one_month_before).zfill(2) + "-" + str(month_two_month_before).zfill(2) + ".inc" , 'w')
                SC_8_4_3_wcs_template = template_wcs_multibans3        
                SC_8_4_3_wcs_template = SC_8_4_3_wcs_template.replace("$INDICE$","SC_8-4-3")
                SC_8_4_3_wcs_template = SC_8_4_3_wcs_template.replace("$ANNO$",str(year_current))
                SC_8_4_3_wcs_template = SC_8_4_3_wcs_template.replace("$MESE$",str(month_current) + "-" + str(month_one_month_before) + "-" + str(month_two_month_before))
                SC_8_4_3_wcs_template = SC_8_4_3_wcs_template.replace("$VRT_PATH$", fileDestination_SC_8_4_3)                
                SC_8_4_3_wcs.write(SC_8_4_3_wcs_template)         
                SC_8_4_3_wcs.close()
                mapfile_wcs_SC_8_4_3_file = open(static_mapfile_wcs_SC_8_4_3 , 'a')  
                mapfile_wcs_SC_8_4_3_string = 'INCLUDE "' + str(include_ogc_path) + "/SC_8-4-3/" + str(year_current) + "/wcs_" + str(month_current).zfill(2)  + "-" + str(month_one_month_before).zfill(2) + "-" + str(month_two_month_before).zfill(2) + '.inc"\n' 
                mapfile_wcs_SC_8_4_3_file.write(mapfile_wcs_SC_8_4_3_string)
                mapfile_wcs_SC_8_4_3_file.close()   
                            
            ####################################################################
            #  TC 8-11-4
            #################################################################### 
            print("Generate SC_8-11-4 image for all the tiles.....................................") # https://gis.stackexchange.com/questions/230553/merging-all-tiles-from-one-directory-using-gdal/230588
            fileDestination_SC_8_11_4 = folderDestination + "/mosaic_SC_8-11-4_" + year_current + "_" + str(month_current) + "-" + str(month_one_month_before) + "-" + str(month_two_month_before) + ".tif"    
            fileDestination_SC_8_11_4_tmp0 = folderDestination + "/mosaic_SC_8-11-4_" + year_current + "_" + str(month_current) + "-" + str(month_one_month_before) + "-" + str(month_two_month_before) + "_tmp0.tif"    
            fileDestination_SC_8_11_4_tmp1 = folderDestination + "/mosaic_SC_8-11-4_" + year_current + "_" + str(month_current) + "-" + str(month_one_month_before) + "-" + str(month_two_month_before) + "_tmp1.tif"    
            gdal_warp_str = '{0}' 
            gdal_warp_process = gdal_warp_str.format(gdalwarp_path)        
            for fileAvailable in filesAvailable_SC_8_11_4:
                gdal_warp_process = gdal_warp_process + " " + str(fileAvailable)
            gdal_warp_process = gdal_warp_process + " " + str(fileDestination_SC_8_11_4_tmp0)     
            print(gdal_warp_process) 
            ret = os.system(gdal_warp_process)    
            print("return value " + str(ret))
            
            print("Initial compress image SC_8_11_4")
            gdal_calc_str = '{0} -of {1} -co {2} -co {3} -co {4} {5} {6}'
            gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff" , "COMPRESS=LZW", "TILED=YES", "BIGTIFF=YES", fileDestination_SC_8_11_4_tmp0, fileDestination_SC_8_11_4_tmp1)
            print(gdal_calc_process)
            os.system(gdal_calc_process)                
            print("Adding overview levels SC_8_11_4")
            gdal_calc_str = '{0} -r average {1} 2 4 8 16 32 64 --config {2} --config {3}'
            gdal_calc_process = gdal_calc_str.format(gdal_gdaladdo_path, fileDestination_SC_8_11_4_tmp1 , "COMPRESS_OVERVIEW JPEG", "INTERLEAVE_OVERVIEW PIXEL")
            print(gdal_calc_process)
            os.system(gdal_calc_process)            
            print("Compress and move image SC_8_11_4")
            gdal_calc_str = '{0} -of {1} -co {2} -co {3} -co {4} -co {5} -co {6} {7} {8}'
            gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff", "COPY_SRC_OVERVIEWS=YES" , "COMPRESS=LZW", "PREDICTOR=2", "TILED=YES", "BIGTIFF=YES", fileDestination_SC_8_11_4_tmp1, fileDestination_SC_8_11_4)            
            print(gdal_calc_process)
            os.system(gdal_calc_process)
            os.remove(fileDestination_SC_8_11_4_tmp0)
            os.remove(fileDestination_SC_8_11_4_tmp1) 
            print(fileDestination_SC_8_11_4)           
            
            for fileAvailable in filesAvailable_SC_8_11_4:
                os.remove(str(fileAvailable))       

            ################################################# TMP TMP #################################################
            month_one_month_before = data_one_month_before.month
            year_one_month_before = data_one_month_before.year               
            datetime_threshold = datetime.datetime(year_before_inc_file_already_available, month_before_inc_file_already_available, 1)
            datetime_data = datetime.datetime(int(year_one_month_before), int(month_one_month_before),1)
            if(datetime_data>=datetime_threshold): # questo per evitare di andare a ricreare nuovamente se vado a rielaborare dati gia fatti !!!!!!!!
            ###if(year>=year_before_inc_file_already_available and month >= month_before_inc_file_already_available): # questo per evitare di andare a ricreare nuovamente se vado a rielaborare dati gia fatti !!!!!!!!
            ################################################# TMP TMP #################################################    
                ####################################################################
                # generate file .inc and fill its for SC 8-11-4  
                ####################################################################                            
                createFolder(include_ogc_path + '/SC_8-11-4/' + str(year_current))
                SC_8_11_4_wms = open(include_ogc_path + '/SC_8-11-4/' + str(year_current) + "/wms_" + str(month_current).zfill(2)  + "-" + str(month_one_month_before).zfill(2) + "-" + str(month_two_month_before).zfill(2) + ".inc" , 'w')        
                SC_8_11_4_wms_template = template_wms        
                SC_8_11_4_wms_template = SC_8_11_4_wms_template.replace("$INDICE$","SC_8-11-4")
                SC_8_11_4_wms_template = SC_8_11_4_wms_template.replace("$ANNO$",str(year_current))
                SC_8_11_4_wms_template = SC_8_11_4_wms_template.replace("$MESE$",str(month_current) + "-" + str(month_one_month_before) + "-" + str(month_two_month_before))
                SC_8_11_4_wms_template = SC_8_11_4_wms_template.replace("$VRT_PATH$", fileDestination_SC_8_11_4)  
                SC_8_11_4_wms_template = SC_8_11_4_wms_template.replace("$BANDS$","1,2,3")
                SC_8_11_4_wms_template = SC_8_11_4_wms_template.replace("$PROCESSING$","SCALE=0,5000")#"SCALE=AUTO")
                SC_8_11_4_wms.write(SC_8_11_4_wms_template)  
                SC_8_11_4_wms.close()
                mapfile_wms_SC_8_11_4_file = open(static_mapfile_wms_SC_8_11_4 , 'a')  
                mapfile_wms_SC_8_11_4_string = 'INCLUDE "' + str(include_ogc_path) + "/SC_8-11-4/" + str(year_current) + "/wms_" + str(month_current).zfill(2)  + "-" + str(month_one_month_before).zfill(2) + "-" + str(month_two_month_before).zfill(2) + '.inc"\n' 
                mapfile_wms_SC_8_11_4_file.write(mapfile_wms_SC_8_11_4_string)
                mapfile_wms_SC_8_11_4_file.close()       
                
                SC_8_11_4_wcs = open(include_ogc_path + '/SC_8-11-4/' + str(year_current) + "/wcs_" + str(month_current).zfill(2)  + "-" + str(month_one_month_before).zfill(2) + "-" + str(month_two_month_before).zfill(2) + ".inc" , 'w')
                SC_8_11_4_wcs_template = template_wcs_multibans3        
                SC_8_11_4_wcs_template = SC_8_11_4_wcs_template.replace("$INDICE$","SC_8-11-4")
                SC_8_11_4_wcs_template = SC_8_11_4_wcs_template.replace("$ANNO$",str(year_current))
                SC_8_11_4_wcs_template = SC_8_11_4_wcs_template.replace("$MESE$",str(month_current) + "-" + str(month_one_month_before) + "-" + str(month_two_month_before))
                SC_8_11_4_wcs_template = SC_8_11_4_wcs_template.replace("$VRT_PATH$", fileDestination_SC_8_11_4)                
                SC_8_11_4_wcs.write(SC_8_11_4_wcs_template)         
                SC_8_11_4_wcs.close()
                mapfile_wcs_SC_8_11_4_file = open(static_mapfile_wcs_SC_8_11_4 , 'a')  
                mapfile_wcs_SC_8_11_4_string = 'INCLUDE "' + str(include_ogc_path) + "/SC_8-11-4/" + str(year_current) + "/wcs_" + str(month_current).zfill(2)  + "-" + str(month_one_month_before).zfill(2) + "-" + str(month_two_month_before).zfill(2) + '.inc"\n' 
                mapfile_wcs_SC_8_11_4_file.write(mapfile_wcs_SC_8_11_4_string)
                mapfile_wcs_SC_8_11_4_file.close()            
                                          
             
            # set the "path_images_zone" into the table scarichi2 to the path where the mosaic image is available
            sql = """
                UPDATE scarichi2 SET path_images_zone = %s WHERE 
                (EXTRACT(MONTH FROM data_acq) = %s AND EXTRACT(YEAR FROM data_acq) = %s OR
                EXTRACT(MONTH FROM data_acq) = %s AND EXTRACT(YEAR FROM data_acq) = %s OR
                EXTRACT(MONTH FROM data_acq) = %s AND EXTRACT(YEAR FROM data_acq) = %s) AND 
                'path_NDVI' IS NOT NULL AND "path_EVI" IS NOT NULL AND "path_NBR" IS NOT NULL AND "path_NDWI" IS NOT NULL AND 
                "path_images_zone" IS NULL;
                """                
            sqldata = (folderDestination, str(month_current), str(year_current), str(month_one_month_before), str(year_one_month_before), str(month_two_month_before), str(year_two_month_before))

            print(sqldata)
            dict_cur.execute(sql, sqldata)
            conn.commit()
    

cur.close()
conn.close()
t_totale = (datetime.datetime.now() - t_0).total_seconds()
print ("process ended: " + convertToSeconds(t_totale) + " sec.")
logDB.scrivi("termiato", "")
logDB.chiudi(True)
log_msg("create mosaic image procedure terminated ")

