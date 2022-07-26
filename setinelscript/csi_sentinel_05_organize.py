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
from osgeo import gdal
from datetime import date, datetime, timedelta
from csi_sentinel_config import *
from csi_sentinel_log import *
from csi_sentinel_utils import *
from csi_sentinel_wcs_wms_template import *


#########################################################################################
def createFolder(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
#########################################################################################
        
 
session = requests.Session()
session.auth = (CSI_user, CSI_password)

printStartScript("start organize files procedure")
logDB.scrivi("in execution", "")


gdal_build_vrt_path  = '/usr/local/bin/gdalbuildvrt'
gdal_translate_path  = '/usr/local/bin/gdal_translate' 
gdal_gdaladdo_path = '/usr/local/bin/gdaladdo'

# open the DB where the information for the download are stored
try:
    conn = psycopg2.connect(dbname=dbname, user=dbuser, host=dbhost, port=dbport, password=dbpass)
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
except:
    print ("ERROR: error in the database connection")

tz = pytz.timezone('Europe/Rome')
current = datetime.datetime.now(tz)
previous_month_month = (current.replace(day=1) - timedelta(days=1)).month # replace current date with the first day of this month and subtract a day to be in the previous month
previous_month_month = str(format(previous_month_month, '02d')) # impose the format of the string rappresenting the month
previous_month_year = str((current.replace(day=1) - timedelta(days=1)).year)  # replace current date with the first day of this month and subtract a day and extract the year
print("Check the data previously year: " + previous_month_year + ", month: " + previous_month_month)

########################################################################################################################################

# extract for all the data already elaborated with a with a data minor to the first day of this month and where path of final images is nulla
sql = 'SELECT * FROM scarichi2 WHERE data_acq < %s  AND data_elaboration_end IS NOT NULL AND ("path_NDVI" IS NULL) AND ("path_EVI" IS NULL) AND ("path_NBR" IS NULL) AND ("path_NDWI" IS NULL)'
sqldata = (current.replace(minute=59, hour=23, day=1) - timedelta(days=1), )
dict_cur.execute(sql, sqldata)
cur = conn.cursor()

entries = []
for row in dict_cur:
    entries.append(row)
print("entries element: " + str(len(entries)))
   
# create a set where al the unique element are the tuple (tile, year, month)
print("tile_month_year_available")
tile_month_year_available = set()
month_year_available = set()
for row in entries:
    #tile_month_year = (str(row["title"][38:44]), str(row["title"][49:51]), str(row["title"][45:49]))
    tile_month_year = (str(row["title"][38:44]), str(row["title"][15:17]), str(row["title"][11:15]))
    tile_month_year_available.add(tile_month_year)
print("tile_month_year_available : " + str(tile_month_year_available))    
print("tile_month_year_available len: " + str(len(tile_month_year_available)))

print("  ") 
print(" ***************************************** ") 
print("  ") 

nr_files_moved = 0
for tile_month_year in tile_month_year_available:    
    sql = "SELECT * FROM scarichi2 WHERE tile = %s AND EXTRACT(MONTH FROM data_acq) = %s AND EXTRACT(YEAR FROM data_acq) = %s AND data_elaboration_end IS NULL"
    tile, month, year = tile_month_year
    print("tile: " + str(tile) + " - month: " + str(month) + " - year: " + str(year))
    sqldata = (tile, month, year)
    dict_cur.execute(sql, sqldata)
    cur = conn.cursor()
    entries = []
    print("entries")
    for row in dict_cur:
        entries.append(row)
        print("    " + str(row))
    print("entries element: " + str(len(entries)))
    # if it is void this implcates that all the data are present and are elaborated...also the data are reay to be moved
    if not len(entries):   
        # move the image file ready to be moved (finisced for that month) in the final path
        print("tile-month-year ready to be moved: " + tile + "-" + month + "-" + year)
        month_year = (month, year)
        month_year_available.add(month_year)
        
        folderIndicatorSource = pathUnzipBase + year +"/"+ month +"/"+ tile
        folderIndicatorDestination = pathSaveFinalImageBase + year +"/"+ month +"/"+ tile         
        ################################################# TMP TMP #################################################
        # se non trovo la cartella temporanea del mese, prendo la cartella del mese corretta (se disponibile) e la faccio diventare la temporanea
        # in questo modo mantengo i vecchi calcoli e posso fare tutto nuovamente. Cosi fancendo trasferisco solo i dati prima volta che faccio tutto e non ho
        # gia effettuato backup dei dati gia presenti.
        datetime_threshold = datetime.datetime(year_before_images_available, month_before_images_available,1)
        datetime_data = datetime.datetime(int(year), int(month),1)
        if(datetime_data<datetime_threshold):        
        #if(year<year_before_images_available):
            folderIndicatorMonthOldBackup = pathSaveFinalImageBase + year +"/"+ month + "_tmp"
            folderIndicatorMonth = pathSaveFinalImageBase + year +"/"+ month
            if not os.path.exists(folderIndicatorMonthOldBackup):
                if os.path.exists(folderIndicatorMonth):
                    os.rename(folderIndicatorMonth, folderIndicatorMonthOldBackup)
        ################################################# TMP TMP #################################################                               
        createFolder(folderIndicatorDestination)
        folderFileListForVrt = pathSaveFinalImageBase + year +"/"+ month
        createFolder(folderFileListForVrt)
        cloudiness_vrt = open(folderFileListForVrt + "/cloudiness_" + year + "_" + month + ".txt" , 'a')
        cloudiness_mask_vrt = open(folderFileListForVrt + "/cloudiness_mask_" + year + "_" + month + ".txt" , 'a')
        EVI_vrt = open(folderFileListForVrt + "/EVI_" + year + "_" + month + ".txt" , 'a')
        mask_vrt = open(folderFileListForVrt + "/mask_" + year + "_" + month + ".txt" , 'a')
        NBR_vrt = open(folderFileListForVrt + "/NBR_" + year + "_" + month + ".txt" , 'a')
        NDVI_vrt = open(folderFileListForVrt + "/NDVI_" + year + "_" + month + ".txt" , 'a')
        NDWI_vrt = open(folderFileListForVrt + "/NDWI_" + year + "_" + month + ".txt" , 'a')
              
     
        fileIndicatorDestination = folderIndicatorDestination + "/cloudiness_" + tile + "_" + year + "_" + month + ".tif"
        fileIndicatorSource = folderIndicatorSource + "/cloudiness_" + tile + "_" + year + "_" + month + ".tif" 
        fileIndicatorSource_tmp = folderIndicatorSource + "/cloudiness_" + tile + "_" + year + "_" + month + "_tmp.tif" 
        print("fileIndicatorSource: " + fileIndicatorSource)
        print("fileIndicatorDestination: " + fileIndicatorDestination)
        print("fileIndicatorSource_tmp: " + fileIndicatorSource_tmp)
        nr_files_moved = nr_files_moved +1

     
        print("Initial compress image cloudiness")
        gdal_calc_str = '{0} -of {1} -co {2} -co {3} {4} {5}'
        gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff" , "COMPRESS=LZW", "TILED=YES", fileIndicatorSource, fileIndicatorSource_tmp)
        print(gdal_calc_process)
        os.system(gdal_calc_process)                    
        print("Adding overview levels cloudiness")
        gdal_calc_str = '{0} -r average {1} 2 4 8 16 32 64 --config {2} --config {3}'
        gdal_calc_process = gdal_calc_str.format(gdal_gdaladdo_path, fileIndicatorSource_tmp , "COMPRESS_OVERVIEW JPEG", "INTERLEAVE_OVERVIEW PIXEL")
        print(gdal_calc_process)
        os.system(gdal_calc_process)                
        print("Compress and move image cloudiness")
        gdal_calc_str = '{0} -of {1} -co {2} -co {3} -co {4} -co {5} {6} {7}'
        gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff", "COPY_SRC_OVERVIEWS=YES" , "COMPRESS=LZW", "PREDICTOR=2", "TILED=YES", fileIndicatorSource_tmp, fileIndicatorDestination)
        print(gdal_calc_process)
        os.system(gdal_calc_process)     
        os.remove(fileIndicatorSource)
        os.remove(fileIndicatorSource_tmp)           
        cloudiness_vrt.write(fileIndicatorDestination + "\n")
        
        
        fileIndicatorDestination = folderIndicatorDestination + "/cloudiness_mask_" + tile + "_" + year + "_" + month + ".tif"
        fileIndicatorSource = folderIndicatorSource + "/cloudiness_mask_" + tile + "_" + year + "_" + month + ".tif" 
        fileIndicatorSource_tmp = folderIndicatorSource + "/cloudiness_mask_" + tile + "_" + year + "_" + month + "_tmp.tif" 
        print("fileIndicatorSource: " + fileIndicatorSource)
        print("fileIndicatorDestination: " + fileIndicatorDestination)
        print("fileIndicatorSource_tmp: " + fileIndicatorSource_tmp)
        nr_files_moved = nr_files_moved +1
        
       
        print("Initial compress image cloudiness_mask")
        gdal_calc_str = '{0} -of {1} -co {2} -co {3} {4} {5}'
        gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff" , "COMPRESS=LZW", "TILED=YES", fileIndicatorSource, fileIndicatorSource_tmp)
        print(gdal_calc_process)
        os.system(gdal_calc_process)                
        print("Adding overview levels cloudiness_mask")
        gdal_calc_str = '{0} -r average {1} 2 4 8 16 32 64 --config {2} --config {3}'
        gdal_calc_process = gdal_calc_str.format(gdal_gdaladdo_path, fileIndicatorSource_tmp , "COMPRESS_OVERVIEW JPEG", "INTERLEAVE_OVERVIEW PIXEL")
        print(gdal_calc_process)
        os.system(gdal_calc_process)            
        print("Compress and move image cloudiness_mask")
        gdal_calc_str = '{0} -of {1} -co {2} -co {3} -co {4} -co {5} {6} {7}'
        gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff", "COPY_SRC_OVERVIEWS=YES" , "COMPRESS=LZW", "PREDICTOR=2", "TILED=YES", fileIndicatorSource_tmp, fileIndicatorDestination)
        print(gdal_calc_process)
        os.system(gdal_calc_process)   
        os.remove(fileIndicatorSource)
        os.remove(fileIndicatorSource_tmp) 
        cloudiness_mask_vrt.write(fileIndicatorDestination + "\n")
        
        
        fileIndicatorDestination = folderIndicatorDestination + "/EVI_" + tile + "_" + year + "_" + month + ".tif"
        fileIndicatorSource = folderIndicatorSource + "/EVI_" + tile + "_" + year + "_" + month + ".tif" 
        fileIndicatorSource_tmp = folderIndicatorSource + "/EVI_" + tile + "_" + year + "_" + month + "_tmp.tif" 
        print("fileIndicatorSource: " + fileIndicatorSource)
        print("fileIndicatorDestination: " + fileIndicatorDestination)
        print("fileIndicatorSource_tmp: " + fileIndicatorSource_tmp)
        nr_files_moved = nr_files_moved +1
        
        print("Initial compress image EVI")
        gdal_calc_str = '{0} -of {1} -co {2} -co {3} {4} {5}'
        gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff" , "COMPRESS=LZW", "TILED=YES", fileIndicatorSource, fileIndicatorSource_tmp)
        print(gdal_calc_process)
        os.system(gdal_calc_process)                
        print("Adding overview levels EVI")
        gdal_calc_str = '{0} -r average {1} 2 4 8 16 32 64 --config {2} --config {3}'
        gdal_calc_process = gdal_calc_str.format(gdal_gdaladdo_path, fileIndicatorSource_tmp , "COMPRESS_OVERVIEW JPEG", "INTERLEAVE_OVERVIEW PIXEL")
        print(gdal_calc_process)
        os.system(gdal_calc_process)            
        print("Compress and move image EVI")
        gdal_calc_str = '{0} -of {1} -co {2} -co {3} -co {4} -co {5} {6} {7}'
        gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff", "COPY_SRC_OVERVIEWS=YES" , "COMPRESS=LZW", "PREDICTOR=2", "TILED=YES", fileIndicatorSource_tmp, fileIndicatorDestination)
        print(gdal_calc_process)
        os.system(gdal_calc_process)  
        os.remove(fileIndicatorSource)
        os.remove(fileIndicatorSource_tmp)            
        EVI_vrt.write(fileIndicatorDestination + "\n")        
        sql = 'UPDATE scarichi2 SET "path_EVI" = %s WHERE (EXTRACT(MONTH FROM data_acq) = %s AND EXTRACT(YEAR FROM data_acq) = %s AND tile = %s)'    
        sqldata = (fileIndicatorDestination, month, year, tile)  
        cur.execute(sql, sqldata)    
        print("query: " + cur.query)            
        conn.commit()    

    
        fileIndicatorDestination = folderIndicatorDestination + "/mask_" + tile + "_" + year + "_" + month + ".tif"
        fileIndicatorSource = folderIndicatorSource + "/mask_" + tile + "_" + year + "_" + month + ".tif" 
        fileIndicatorSource_tmp = folderIndicatorSource + "/mask_" + tile + "_" + year + "_" + month + "_tmp.tif" 
        print("fileIndicatorSource: " + fileIndicatorSource)
        print("fileIndicatorDestination: " + fileIndicatorDestination)
        print("fileIndicatorSource_tmp: " + fileIndicatorSource_tmp) 
        nr_files_moved = nr_files_moved +1
        
        print("Initial compress image mask")
        gdal_calc_str = '{0} -of {1} -co {2} -co {3} {4} {5}'
        gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff" , "COMPRESS=LZW", "TILED=YES", fileIndicatorSource, fileIndicatorSource_tmp)
        print(gdal_calc_process)
        os.system(gdal_calc_process)               
        print("Adding overview levels mask")
        gdal_calc_str = '{0} -r average {1} 2 4 8 16 32 64 --config {2} --config {3}'
        gdal_calc_process = gdal_calc_str.format(gdal_gdaladdo_path, fileIndicatorSource_tmp , "COMPRESS_OVERVIEW JPEG", "INTERLEAVE_OVERVIEW PIXEL")
        print(gdal_calc_process)
        os.system(gdal_calc_process)            
        print("Compress and move image mask")
        gdal_calc_str = '{0} -of {1} -co {2} -co {3} -co {4} -co {5} {6} {7}'
        gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff", "COPY_SRC_OVERVIEWS=YES" , "COMPRESS=LZW", "PREDICTOR=2", "TILED=YES", fileIndicatorSource_tmp, fileIndicatorDestination)
        print(gdal_calc_process)
        os.system(gdal_calc_process)   
        os.remove(fileIndicatorSource)
        os.remove(fileIndicatorSource_tmp)           
        mask_vrt.write(fileIndicatorDestination + "\n")              
 
               
        fileIndicatorDestination = folderIndicatorDestination + "/NBR_" + tile + "_" + year + "_" + month + ".tif"
        fileIndicatorSource = folderIndicatorSource + "/NBR_" + tile + "_" + year + "_" + month + ".tif" 
        fileIndicatorSource_tmp = folderIndicatorSource + "/NBR_" + tile + "_" + year + "_" + month + "_tmp.tif" 
        print("fileIndicatorSource: " + fileIndicatorSource)
        print("fileIndicatorDestination: " + fileIndicatorDestination)
        print("fileIndicatorSource_tmp: " + fileIndicatorSource_tmp)   
        nr_files_moved = nr_files_moved +1
        
        print("Initial compress image NBR")
        gdal_calc_str = '{0} -of {1} -co {2} -co {3} {4} {5}'
        gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff" , "COMPRESS=LZW", "TILED=YES", fileIndicatorSource, fileIndicatorSource_tmp)
        print(gdal_calc_process)
        os.system(gdal_calc_process)               
        print("Adding overview levels NBR")
        gdal_calc_str = '{0} -r average {1} 2 4 8 16 32 64 --config {2} --config {3}'
        gdal_calc_process = gdal_calc_str.format(gdal_gdaladdo_path, fileIndicatorSource_tmp , "COMPRESS_OVERVIEW JPEG", "INTERLEAVE_OVERVIEW PIXEL")
        print(gdal_calc_process)
        os.system(gdal_calc_process)            
        print("Compress and move image NBR")
        gdal_calc_str = '{0} -of {1} -co {2} -co {3} -co {4} -co {5} {6} {7}'
        gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff", "COPY_SRC_OVERVIEWS=YES" , "COMPRESS=LZW", "PREDICTOR=2", "TILED=YES", fileIndicatorSource_tmp, fileIndicatorDestination)
        print(gdal_calc_process)
        os.system(gdal_calc_process)
        os.remove(fileIndicatorSource)
        os.remove(fileIndicatorSource_tmp)              
        NBR_vrt.write(fileIndicatorDestination + "\n")          
        sql = 'UPDATE scarichi2 SET "path_NBR" = %s WHERE (EXTRACT(MONTH FROM data_acq) = %s AND EXTRACT(YEAR FROM data_acq) = %s AND tile = %s)'    
        sqldata = (fileIndicatorDestination, month, year, tile)  
        cur.execute(sql, sqldata) 
        print("query: " + cur.query)               
        conn.commit()    
        
        
   
        fileIndicatorDestination = folderIndicatorDestination + "/NDVI_" + tile + "_" + year + "_" + month + ".tif"
        fileIndicatorSource = folderIndicatorSource + "/NDVI_" + tile + "_" + year + "_" + month + ".tif" 
        fileIndicatorSource_tmp = folderIndicatorSource + "/NDVI_" + tile + "_" + year + "_" + month + "_tmp.tif" 
        print("fileIndicatorSource: " + fileIndicatorSource)
        print("fileIndicatorDestination: " + fileIndicatorDestination)
        print("fileIndicatorSource_tmp: " + fileIndicatorSource_tmp) 
        nr_files_moved = nr_files_moved +1         
        
        print("Initial compress image NDVI")
        gdal_calc_str = '{0} -of {1} -co {2} -co {3} {4} {5}'
        gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff" , "COMPRESS=LZW", "TILED=YES", fileIndicatorSource, fileIndicatorSource_tmp)
        print(gdal_calc_process)
        os.system(gdal_calc_process)                
        print("Adding overview levels NDVI")
        gdal_calc_str = '{0} -r average {1} 2 4 8 16 32 64 --config {2} --config {3}'
        gdal_calc_process = gdal_calc_str.format(gdal_gdaladdo_path, fileIndicatorSource_tmp , "COMPRESS_OVERVIEW JPEG", "INTERLEAVE_OVERVIEW PIXEL")
        print(gdal_calc_process)
        os.system(gdal_calc_process)            
        print("Compress and move image NDVI")
        gdal_calc_str = '{0} -of {1} -co {2} -co {3} -co {4} -co {5} {6} {7}'
        gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff", "COPY_SRC_OVERVIEWS=YES" , "COMPRESS=LZW", "PREDICTOR=2", "TILED=YES", fileIndicatorSource_tmp, fileIndicatorDestination)
        print(gdal_calc_process)
        os.system(gdal_calc_process) 
        os.remove(fileIndicatorSource)
        os.remove(fileIndicatorSource_tmp)              
        NDVI_vrt.write(fileIndicatorDestination + "\n")         
        sql = 'UPDATE scarichi2 SET "path_NDVI" = %s WHERE (EXTRACT(MONTH FROM data_acq) = %s AND EXTRACT(YEAR FROM data_acq) = %s AND tile = %s)'    
        sqldata = (fileIndicatorDestination, month, year, tile)  
        cur.execute(sql, sqldata)   
        print("query: " + cur.query)              
        conn.commit()    


        fileIndicatorDestination = folderIndicatorDestination + "/NDWI_" + tile + "_" + year + "_" + month + ".tif"
        fileIndicatorSource = folderIndicatorSource + "/NDWI_" + tile + "_" + year + "_" + month + ".tif"         
        fileIndicatorSource_tmp = folderIndicatorSource + "/NDWI_" + tile + "_" + year + "_" + month + "_tmp.tif" 
        print("fileIndicatorSource: " + fileIndicatorSource)
        print("fileIndicatorDestination: " + fileIndicatorDestination)
        print("fileIndicatorSource_tmp: " + fileIndicatorSource_tmp) 
        nr_files_moved = nr_files_moved +1
        
        print("Initial compress image NDWI")
        gdal_calc_str = '{0} -of {1} -co {2} -co {3} {4} {5}'
        gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff" , "COMPRESS=LZW", "TILED=YES", fileIndicatorSource, fileIndicatorSource_tmp)
        print(gdal_calc_process)
        os.system(gdal_calc_process)               
        print("Adding overview levels NDWI")
        gdal_calc_str = '{0} -r average {1} 2 4 8 16 32 64 --config {2} --config {3}'
        gdal_calc_process = gdal_calc_str.format(gdal_gdaladdo_path, fileIndicatorSource_tmp , "COMPRESS_OVERVIEW JPEG", "INTERLEAVE_OVERVIEW PIXEL")
        print(gdal_calc_process)
        os.system(gdal_calc_process)            
        print("Compress and move image NDWI")
        gdal_calc_str = '{0} -of {1} -co {2} -co {3} -co {4} -co {5} {6} {7}'
        gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff", "COPY_SRC_OVERVIEWS=YES" , "COMPRESS=LZW", "PREDICTOR=2", "TILED=YES", fileIndicatorSource_tmp, fileIndicatorDestination)
        print(gdal_calc_process)
        os.system(gdal_calc_process)   
        os.remove(fileIndicatorSource)
        os.remove(fileIndicatorSource_tmp)           
        NDWI_vrt.write(fileIndicatorDestination + "\n")        
        sql = 'UPDATE scarichi2 SET "path_NDWI" = %s WHERE (EXTRACT(MONTH FROM data_acq) = %s AND EXTRACT(YEAR FROM data_acq) = %s AND tile = %s)'    
        sqldata = (fileIndicatorDestination, month, year, tile)  
        cur.execute(sql, sqldata)    
        print("query: " + cur.query)            
        conn.commit()    
                     
        
        cloudiness_vrt.close()
        cloudiness_mask_vrt.close()
        EVI_vrt.close()
        mask_vrt.close()
        NBR_vrt.close()
        NDVI_vrt.close()
        NDWI_vrt.close()
        

    
####################################################################
# creation file vrt using the txt file filled with the series of tif images compose the vrt
####################################################################       
print("------------------------------------------------------")       
print("------------------------------------------------------") 
print("month_year_available len: " + str(len(month_year_available)))
print("month_year_available : " + str(month_year_available))   
month_year_available = sorted(month_year_available, key=lambda tup: tup[1]+tup[0]) 
print("month_year_available sorted: " + str(month_year_available)) 

for month_year in month_year_available: 
    month, year = month_year
    ########################################################################
    # Generate VRT for cloudiness
    ########################################################################
    folderFileListForVrt = pathSaveFinalImageBase + year +"/"+ month
    print("Generate VRT for cloudiness.....................................")
    gdal_calc_str = '{0} -input_file_list {1} -overwrite {2}'
    vrt_list_files = folderFileListForVrt + "/cloudiness_" + year + "_" + month + ".txt"
    vrt_output_file = folderFileListForVrt + "/cloudiness_" + year + "_" + month + ".vrt"
    gdal_calc_process = gdal_calc_str.format(gdal_build_vrt_path, vrt_list_files, vrt_output_file)
    print(gdal_calc_process)
    ret = os.system(gdal_calc_process)    
    print("\n\nRET RET RET " + str(ret))      
    ########################################################################
    # Generate VRT for cloudiness
    ########################################################################
    folderFileListForVrt = pathSaveFinalImageBase + year +"/"+ month
    print("Generate VRT for cloudiness_mask.....................................")
    gdal_calc_str = '{0} -input_file_list {1} -overwrite {2}'
    vrt_list_files = folderFileListForVrt + "/cloudiness_mask_" + year + "_" + month + ".txt"
    vrt_output_file = folderFileListForVrt + "/cloudiness_mask_" + year + "_" + month + ".vrt"
    gdal_calc_process = gdal_calc_str.format(gdal_build_vrt_path, vrt_list_files, vrt_output_file)
    print(gdal_calc_process)
    ret = os.system(gdal_calc_process)    
    print("\n\nRET RET RET " + str(ret))       
    ########################################################################
    # Generate VRT for EVI
    ########################################################################
    folderFileListForVrt = pathSaveFinalImageBase + year +"/"+ month
    print("Generate VRT for EVI.....................................")
    gdal_calc_str = '{0} -input_file_list {1} -overwrite {2}'
    vrt_list_files = folderFileListForVrt + "/EVI_" + year + "_" + month + ".txt"
    vrt_output_file = folderFileListForVrt + "/EVI_" + year + "_" + month + ".vrt"
    gdal_calc_process = gdal_calc_str.format(gdal_build_vrt_path, vrt_list_files, vrt_output_file)
    print(gdal_calc_process)
    ret = os.system(gdal_calc_process)    
    print("\n\nRET RET RET " + str(ret))       
    ########################################################################
    # Generate VRT for mask
    ########################################################################
    folderFileListForVrt = pathSaveFinalImageBase + year +"/"+ month
    print("Generate VRT for mask.....................................")
    gdal_calc_str = '{0} -input_file_list {1} -overwrite {2}'
    vrt_list_files = folderFileListForVrt + "/mask_" + year + "_" + month + ".txt"
    vrt_output_file = folderFileListForVrt + "/mask_" + year + "_" + month + ".vrt"
    gdal_calc_process = gdal_calc_str.format(gdal_build_vrt_path, vrt_list_files, vrt_output_file)
    print(gdal_calc_process)
    ret = os.system(gdal_calc_process)    
    print("\n\nRET RET RET " + str(ret))        
    ########################################################################
    # Generate VRT for NBR
    ########################################################################
    folderFileListForVrt = pathSaveFinalImageBase + year +"/"+ month
    print("Generate VRT for NBR.....................................")
    gdal_calc_str = '{0} -input_file_list {1} -overwrite {2}'
    vrt_list_files = folderFileListForVrt + "/NBR_" + year + "_" + month + ".txt"
    vrt_output_file = folderFileListForVrt + "/NBR_" + year + "_" + month + ".vrt"
    gdal_calc_process = gdal_calc_str.format(gdal_build_vrt_path, vrt_list_files, vrt_output_file)
    print(gdal_calc_process)
    ret = os.system(gdal_calc_process)    
    print("\n\nRET RET RET " + str(ret))   
    ########################################################################
    # Generate VRT for NDVI
    ########################################################################
    folderFileListForVrt = pathSaveFinalImageBase + year +"/"+ month
    print("Generate VRT for NDVI.....................................")
    gdal_calc_str = '{0} -input_file_list {1} -overwrite {2}'
    vrt_list_files = folderFileListForVrt + "/NDVI_" + year + "_" + month + ".txt"
    vrt_output_file = folderFileListForVrt + "/NDVI_" + year + "_" + month + ".vrt"
    gdal_calc_process = gdal_calc_str.format(gdal_build_vrt_path, vrt_list_files, vrt_output_file)
    print(gdal_calc_process)
    ret = os.system(gdal_calc_process)    
    print("\n\nRET RET RET " + str(ret))         
    ########################################################################
    # Generate VRT for NDWI
    ########################################################################
    folderFileListForVrt = pathSaveFinalImageBase + year +"/"+ month
    print("Generate VRT for NDWI.....................................")
    gdal_calc_str = '{0} -input_file_list {1} -overwrite {2}'
    vrt_list_files = folderFileListForVrt + "/NDWI_" + year + "_" + month + ".txt"
    vrt_output_file = folderFileListForVrt + "/NDWI_" + year + "_" + month + ".vrt"
    gdal_calc_process = gdal_calc_str.format(gdal_build_vrt_path, vrt_list_files, vrt_output_file)
    print(gdal_calc_process)
    ret = os.system(gdal_calc_process)    
    print("\n\nRET RET RET " + str(ret))                
    
    ################################################# TMP TMP #################################################
    datetime_threshold = datetime.datetime(year_before_inc_file_already_available, month_before_inc_file_already_available,1)
    datetime_data = datetime.datetime(int(year), int(month),1)
    if(datetime_data>=datetime_threshold): # questo per evitare di andare a ricreare nuovamente se vado a rielaborare dati gia fatti !!!!!!!!
    ###if(year>=year_before_inc_file_already_available and month >= month_before_inc_file_already_available): # questo per evitare di andare a ricreare nuovamente se vado a rielaborare dati gia fatti !!!!!!!!
    ################################################# TMP TMP #################################################
        ####################################################################
        # generate file .inc and fill its for EVI
        ####################################################################     
        createFolder(include_ogc_path + '/EVI/' + year)
        
        EVI_wms = open(include_ogc_path + '/EVI/' + year + "/wms_" + str(month).zfill(2) + ".inc" , 'w')        
        EVI_wms_template = template_wms        
        EVI_wms_template = EVI_wms_template.replace("$INDICE$","EVI")
        EVI_wms_template = EVI_wms_template.replace("$ANNO$",str(year))
        EVI_wms_template = EVI_wms_template.replace("$MESE$",str(month))
        EVI_wms_template = EVI_wms_template.replace("$VRT_PATH$", folderFileListForVrt + "/EVI_" + year + "_" + month + ".vrt")  
        EVI_wms_template = EVI_wms_template.replace("$BANDS$","1")
        EVI_wms_template = EVI_wms_template.replace("$PROCESSING$","SCALE=-30000,30000")#"SCALE=AUTO")
        EVI_wms.write(EVI_wms_template) 
        EVI_wms.close()
        mapfile_wms_EVI_file = open(static_mapfile_wms_EVI , 'a')     
        mapfile_wms_EVI_string = 'INCLUDE "' + str(include_ogc_path) + "/EVI/" + str(year) + "/wms_" + str(month).zfill(2) + '.inc"\n'
        mapfile_wms_EVI_file.write(mapfile_wms_EVI_string)
        mapfile_wms_EVI_file.close()       
        
        EVI_wcs = open(include_ogc_path + '/EVI/' + year + "/wcs_" + str(month).zfill(2) + ".inc" , 'w')
        EVI_wcs_template = template_wcs        
        EVI_wcs_template = EVI_wcs_template.replace("$INDICE$","EVI")
        EVI_wcs_template = EVI_wcs_template.replace("$ANNO$",str(year))
        EVI_wcs_template = EVI_wcs_template.replace("$MESE$",str(month))
        EVI_wcs_template = EVI_wcs_template.replace("$VRT_PATH$", folderFileListForVrt + "/EVI_" + year + "_" + month + ".vrt")                
        EVI_wcs.write(EVI_wcs_template)         
        EVI_wcs.close()
        mapfile_wcs_EVI_file = open(static_mapfile_wcs_EVI , 'a')        
        mapfile_wcs_EVI_string = 'INCLUDE "' + str(include_ogc_path) + "/EVI/" + str(year) + "/wcs_" + str(month).zfill(2) + '.inc"\n'   
        mapfile_wcs_EVI_file.write(mapfile_wcs_EVI_string)
        mapfile_wcs_EVI_file.close()    
       
        
        ####################################################################
        # generate file .inc and fill its for NBR
        #################################################################### 
        createFolder(include_ogc_path + '/NBR/' + year)
        
        NBR_wms = open(include_ogc_path + '/NBR/' + year + "/wms_" + str(month).zfill(2) + ".inc" , 'w')        
        NBR_wms_template = template_wms        
        NBR_wms_template = NBR_wms_template.replace("$INDICE$","NBR")
        NBR_wms_template = NBR_wms_template.replace("$ANNO$",str(year))
        NBR_wms_template = NBR_wms_template.replace("$MESE$",str(month))
        NBR_wms_template = NBR_wms_template.replace("$VRT_PATH$", folderFileListForVrt + "/NBR_" + year + "_" + month + ".vrt")
        NBR_wms_template = NBR_wms_template.replace("$BANDS$","1")
        NBR_wms_template = NBR_wms_template.replace("$PROCESSING$","SCALE=-11000,11000")#"SCALE=AUTO")
        NBR_wms.write(NBR_wms_template) 
        NBR_wms.close()
        mapfile_wms_NBR_file = open(static_mapfile_wms_NBR , 'a')  
        mapfile_wms_NBR_string = 'INCLUDE "' + str(include_ogc_path) + "/NBR/" + str(year) + "/wms_" + str(month).zfill(2) + '.inc"\n'
        mapfile_wms_NBR_file.write(mapfile_wms_NBR_string)
        mapfile_wms_NBR_file.close()     
        
        NBR_wcs = open(include_ogc_path + '/NBR/' + year + "/wcs_" + str(month).zfill(2) + ".inc" , 'w')
        NBR_wcs_template = template_wcs        
        NBR_wcs_template = NBR_wcs_template.replace("$INDICE$","NBR")
        NBR_wcs_template = NBR_wcs_template.replace("$ANNO$",str(year))
        NBR_wcs_template = NBR_wcs_template.replace("$MESE$",str(month))
        NBR_wcs_template = NBR_wcs_template.replace("$VRT_PATH$", folderFileListForVrt + "/NBR_" + year + "_" + month + ".vrt")                
        NBR_wcs.write(NBR_wcs_template)         
        NBR_wcs.close()
        mapfile_wcs_NBR_file = open(static_mapfile_wcs_NBR , 'a')          
        mapfile_wcs_NBR_string = 'INCLUDE "' + str(include_ogc_path) + "/NBR/" + str(year) + "/wcs_" + str(month).zfill(2) + '.inc"\n'
        mapfile_wcs_NBR_file.write(mapfile_wcs_NBR_string)
        mapfile_wcs_NBR_file.close()       
        
        
        ####################################################################
        # generate file .inc and fill its for NDVI
        #################################################################### 
        createFolder(include_ogc_path + '/NDVI/' + year)
        
        NDVI_wms = open(include_ogc_path + '/NDVI/' + year + "/wms_" + str(month).zfill(2) + ".inc" , 'w')        
        NDVI_wms_template = template_wms        
        NDVI_wms_template = NDVI_wms_template.replace("$INDICE$","NDVI")
        NDVI_wms_template = NDVI_wms_template.replace("$ANNO$",str(year))
        NDVI_wms_template = NDVI_wms_template.replace("$MESE$",str(month))
        NDVI_wms_template = NDVI_wms_template.replace("$VRT_PATH$", folderFileListForVrt + "/NDVI_" + year + "_" + month + ".vrt")   
        NDVI_wms_template = NDVI_wms_template.replace("$BANDS$","1")
        NDVI_wms_template = NDVI_wms_template.replace("$PROCESSING$","SCALE=0,11000")#"SCALE=AUTO")
        NDVI_wms.write(NDVI_wms_template) 
        NDVI_wms.close()
        mapfile_wms_NDVI_file = open(static_mapfile_wms_NDVI , 'a')
        mapfile_wms_NDVI_string = 'INCLUDE "' + str(include_ogc_path) + "/NDVI/" + str(year) + "/wms_" + str(month).zfill(2) + '.inc"\n'
        mapfile_wms_NDVI_file.write(mapfile_wms_NDVI_string)
        mapfile_wms_NDVI_file.close()    
        
        NDVI_wcs = open(include_ogc_path + '/NDVI/' + year + "/wcs_" + str(month).zfill(2) + ".inc" , 'w')
        NDVI_wcs_template = template_wcs        
        NDVI_wcs_template = NDVI_wcs_template.replace("$INDICE$","NDVI")
        NDVI_wcs_template = NDVI_wcs_template.replace("$ANNO$",str(year))
        NDVI_wcs_template = NDVI_wcs_template.replace("$MESE$",str(month))
        NDVI_wcs_template = NDVI_wcs_template.replace("$VRT_PATH$", folderFileListForVrt + "/NDVI_" + year + "_" + month + ".vrt")                
        NDVI_wcs.write(NDVI_wcs_template)         
        NDVI_wcs.close()        
        mapfile_wcs_NDVI_file = open(static_mapfile_wcs_NDVI, 'a')  
        mapfile_wcs_NDVI_string = 'INCLUDE "' + str(include_ogc_path) + "/NDVI/" + str(year) + "/wcs_" + str(month).zfill(2) + '.inc"\n'
        mapfile_wcs_NDVI_file.write(mapfile_wcs_NDVI_string)
        mapfile_wcs_NDVI_file.close()    
        
        
        ####################################################################
        # generate file .inc and fill its for NDWI
        #################################################################### 
        createFolder(include_ogc_path + '/NDWI/' + year)
        
        NDWI_wms = open(include_ogc_path + '/NDWI/' + year + "/wms_" + str(month).zfill(2) + ".inc" , 'w')        
        NDWI_wms_template = template_wms        
        NDWI_wms_template = NDWI_wms_template.replace("$INDICE$","NDWI")
        NDWI_wms_template = NDWI_wms_template.replace("$ANNO$",str(year))
        NDWI_wms_template = NDWI_wms_template.replace("$MESE$",str(month))
        NDWI_wms_template = NDWI_wms_template.replace("$VRT_PATH$", folderFileListForVrt + "/NDWI_" + year + "_" + month + ".vrt")   
        NDWI_wms_template = NDWI_wms_template.replace("$BANDS$","1")
        NDWI_wms_template = NDWI_wms_template.replace("$PROCESSING$","SCALE=-11000,5000")#"SCALE=AUTO")
        NDWI_wms.write(NDWI_wms_template)
        NDWI_wms.close()
        mapfile_wms_NDWI_file = open(static_mapfile_wms_NDWI , 'a')  
        mapfile_wms_NDWI_string = 'INCLUDE "' + str(include_ogc_path) + "/NDWI/" + str(year) + "/wms_" + str(month).zfill(2) + '.inc"\n'
        mapfile_wms_NDWI_file.write(mapfile_wms_NDWI_string)
        mapfile_wms_NDWI_file.close()
            
        NDWI_wcs = open(include_ogc_path + '/NDWI/' + year + "/wcs_" + str(month).zfill(2) + ".inc" , 'w')
        NDWI_wcs_template = template_wcs        
        NDWI_wcs_template = NDWI_wcs_template.replace("$INDICE$","NDWI")
        NDWI_wcs_template = NDWI_wcs_template.replace("$ANNO$",str(year))
        NDWI_wcs_template = NDWI_wcs_template.replace("$MESE$",str(month))
        NDWI_wcs_template = NDWI_wcs_template.replace("$VRT_PATH$", folderFileListForVrt + "/NDWI_" + year + "_" + month + ".vrt")                
        NDWI_wcs.write(NDWI_wcs_template)         
        NDVI_wcs.close()    
        mapfile_wcs_NDWI_file = open(static_mapfile_wcs_NDWI , 'a')     
        mapfile_wcs_NDWI_string = 'INCLUDE "' + str(include_ogc_path) + "/NDWI/" + str(year) + "/wcs_" + str(month).zfill(2) + '.inc"\n'
        mapfile_wcs_NDWI_file.write(mapfile_wcs_NDWI_string)
        mapfile_wcs_NDWI_file.close()
    
    
        ####################################################################
        # generate file .inc and fill its for metainfo_
        ####################################################################     
        createFolder(include_ogc_path + '/metainfo/' + year)
        
        metainfo_wms = open(include_ogc_path + '/metainfo/' + year + "/wms_" + str(month).zfill(2) + ".inc" , 'w')        
        metainfo_wms_template = template_wms_metainfo_cloudiness_mask        
        metainfo_wms_template = metainfo_wms_template.replace("$INDICE$","cloudiness_mask")
        metainfo_wms_template = metainfo_wms_template.replace("$ANNO$",str(year))
        metainfo_wms_template = metainfo_wms_template.replace("$MESE$",str(month))
        metainfo_wms_template = metainfo_wms_template.replace("$VRT_PATH$", folderFileListForVrt + "/cloudiness_mask_" + year + "_" + month + ".vrt")   
        metainfo_wms_template = metainfo_wms_template.replace("$BANDS$","1")
        metainfo_wms_template = metainfo_wms_template.replace("$PROCESSING$","SCALE_1=0,11")
        metainfo_wms.write(metainfo_wms_template)
        metainfo_wms.write('\n')
        
        metainfo_wms_template = template_wms_metainfo        
        metainfo_wms_template = metainfo_wms_template.replace("$INDICE$","cloudiness")
        metainfo_wms_template = metainfo_wms_template.replace("$ANNO$",str(year))
        metainfo_wms_template = metainfo_wms_template.replace("$MESE$",str(month))
        metainfo_wms_template = metainfo_wms_template.replace("$VRT_PATH$", folderFileListForVrt + "/cloudiness_" + year + "_" + month + ".vrt")   
        metainfo_wms_template = metainfo_wms_template.replace("$BANDS$","1,2,3")
        metainfo_wms_template = metainfo_wms_template.replace("$PROCESSING$","SCALE=0,14")#"SCALE=AUTO")
        metainfo_wms.write(metainfo_wms_template)
        metainfo_wms.write('\n')
    
        metainfo_wms_template = template_wms_metainfo        
        metainfo_wms_template = metainfo_wms_template.replace("$INDICE$","mask")
        metainfo_wms_template = metainfo_wms_template.replace("$ANNO$",str(year))
        metainfo_wms_template = metainfo_wms_template.replace("$MESE$",str(month))
        metainfo_wms_template = metainfo_wms_template.replace("$VRT_PATH$", folderFileListForVrt + "/mask_" + year + "_" + month + ".vrt")   
        metainfo_wms_template = metainfo_wms_template.replace("$BANDS$","1")
        metainfo_wms_template = metainfo_wms_template.replace("$PROCESSING$","SCALE=AUTO")
        metainfo_wms.write(metainfo_wms_template)
        
        metainfo_wms.close()
        
        mapfile_wms_metainfo_file = open(static_mapfile_wms_metainfo , 'a')    
        mapfile_wms_metainfo_string = 'INCLUDE "' + str(include_ogc_path) + "/metainfo/" + str(year) + "/wms_" + str(month).zfill(2) + '.inc"\n'
        mapfile_wms_metainfo_file.write(mapfile_wms_metainfo_string)      
        mapfile_wms_metainfo_file.close()
        
   
print("------------------------------------------------------")       
print("------------------------------------------------------") 
print("") 
cur.close()
conn.close()

logDB.scrivi(str(nr_files_moved) + " files moved", "")
logDB.chiudi(True)
log_msg("organize procedure terminated " + str(nr_files_moved) + " files moved")
