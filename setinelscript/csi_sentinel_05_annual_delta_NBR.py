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

printStartScript("start annual deltaNBR procedure")
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
previous_month_month = (current.replace(minute=59, hour=23, day=1) - timedelta(days=1)).month # replace current date with the first day of this month and subtract a day to be in the previous month
previous_month_month = str(format(previous_month_month, '02d')) # impose the format of the string rappresenting the month
previous_month_year = str((current.replace(minute=59, hour=23, day=1) - timedelta(days=1)).year)  # replace current date with the first day of this month and subtract a day and extract the year
print("Check the data previously year: " + previous_month_year + ", month: " + previous_month_month)

########################################################################################################################################

# extract for all the data already elaborated with a with a data minor to the first day of this month and where path of final annualdeltaNBR images is null
sql = 'SELECT * FROM scarichi2 WHERE data_acq < %s  AND data_elaboration_end IS NOT NULL AND ("path_annualdeltaNBR" IS NULL) AND (EXTRACT(YEAR FROM data_acq) > %s) AND (EXTRACT(MONTH FROM data_acq) IN  (%s,%s)) ORDER BY data_acq ASC'
sqldata = (current.replace(minute=59, hour=23, day=1) - timedelta(days=1), str(first_data_available_year_annualNBR), str(month_annualNBR[0]), str(month_annualNBR[1]))
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
    print("first entries element: " + str(len(entries)))
    
    previous_year = int(year) - 1
                
    sql = "SELECT * FROM scarichi2 WHERE tile = %s AND EXTRACT(MONTH FROM data_acq) = %s AND EXTRACT(YEAR FROM data_acq) = %s AND data_elaboration_end IS NULL"
    tile, month, year = tile_month_year
    print("tile: " + str(tile) + " - month: " + str(month) + " - year: " + str(previous_year))
    sqldata = (tile, month, previous_year)
    dict_cur.execute(sql, sqldata)
    cur = conn.cursor()
    for row in dict_cur:
        entries.append(row)
        print("    " + str(row))
    print("second entries element: " + str(len(entries)))    
    
        
    # if it is void this implcates that all the data are present and are elaborated, for this and for the previous month...also the annual_delta_NBR is ready to be created
    if not len(entries):   
        # move the image file ready to be moved (finisced for that month) in the final path
        print("----  tile-month-year ready to be created deltaNBR: " + tile + "-" + month + "-" + year)
        month_year = (month, year)
        month_year_available.add(month_year)
        
        folderIndicatorSource = pathUnzipBase + year +"/"+ month +"/"+ tile
        folderIndicatorDestination = pathSaveFinalImageBase + year +"/"+ month +"/"+ tile  
        
        folderIndicatorSourceCurrent = pathSaveFinalImageBase  + year +"/"+ str(month).zfill(2)  +"/"+ tile
        fileIndicatorCurrentNBR = folderIndicatorSourceCurrent + "/NBR_" + tile + "_" + str(year) + "_" + str(month).zfill(2)  + ".tif"
                                            
        folderIndicatorSourcePrevious = pathSaveFinalImageBase + str(previous_year) +"/"+ str(month).zfill(2)   +"/"+ tile
        fileIndicatorPreviousNBR = folderIndicatorSourcePrevious + "/NBR_" + tile + "_" + str(previous_year) + "_" + str(month).zfill(2)  + ".tif"        
               
        fileIndicatorAnnualDeltaNBR = folderIndicatorDestination + "/annual_delta_NBR_" + tile + "_" + str(year) + "_" + str(month).zfill(2)  + ".tif"
        
        print("fileIndicatorPreviousNBR " + fileIndicatorPreviousNBR)
        print("fileIndicatorCurrentNBR " + fileIndicatorCurrentNBR) 
        
        if os.path.isfile(fileIndicatorPreviousNBR) and os.path.isfile(fileIndicatorCurrentNBR):
            print("files soure A and B available")    
              
            createFolder(folderIndicatorDestination)
            folderFileListForVrt = pathSaveFinalImageBase + year +"/"+ month
            createFolder(folderFileListForVrt)
            annual_delta_NBR_vrt = open(folderFileListForVrt + "/annual_delta_NBR_" + year + "_" + month + ".txt" , 'a')
                      
            #create the annual delta NBR-delta 
            print("Generate annual delta NBR.....................................")   
            calc_expr_annual_delta_NBR = '"((A-B) * (logical_and(A!=-32000,B!=-32000)))  + (-32000 * logical_or(A==0, B==0))"'
            gdal_calc_path  = '/usr/local/bin/gdal_calc.py'
            nodata = '-32000'
            typeof = '"Int16"'
                           
            gdal_calc_str = '{0} -A {1} -B {2} --outfile={3} --calc={4} --NoDataValue={5} --type={6} --debug'
            gdal_calc_process = gdal_calc_str.format(gdal_calc_path, fileIndicatorPreviousNBR, fileIndicatorCurrentNBR, fileIndicatorAnnualDeltaNBR, calc_expr_annual_delta_NBR, nodata, typeof)
            os.system(gdal_calc_process)
            print("Done current with the previous")               
                                 
            fileIndicatorDestination = folderIndicatorDestination + "/annual_delta_NBR_" + tile + "_" + year + "_" + month + ".tif"
            fileIndicatorSource = fileIndicatorAnnualDeltaNBR        
            fileIndicatorSource_tmp = folderIndicatorSource + "/annual_delta_NBR_" + tile + "_" + year + "_" + month + "_tmp.tif" 
            print("fileIndicatorSource: " + fileIndicatorSource)
            print("fileIndicatorDestination: " + fileIndicatorDestination)
            print("fileIndicatorSource_tmp: " + fileIndicatorSource_tmp) 
            nr_files_moved = nr_files_moved +1
            
            print("Initial compress image annual_delta_NBR")
            gdal_calc_str = '{0} -of {1} -co {2} -co {3} {4} {5}'
            gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff" , "COMPRESS=LZW", "TILED=YES", fileIndicatorSource, fileIndicatorSource_tmp)
            print(gdal_calc_process)
            os.system(gdal_calc_process)               
            print("Adding overview levels annual_delta_NBR")
            gdal_calc_str = '{0} -r average {1} 2 4 8 16 32 64 --config {2} --config {3}'
            gdal_calc_process = gdal_calc_str.format(gdal_gdaladdo_path, fileIndicatorSource_tmp , "COMPRESS_OVERVIEW JPEG", "INTERLEAVE_OVERVIEW PIXEL")
            print(gdal_calc_process)
            os.system(gdal_calc_process)            
            print("Compress and move image annual_delta_NBR")
            gdal_calc_str = '{0} -of {1} -co {2} -co {3} -co {4} -co {5} {6} {7}'
            gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff", "COPY_SRC_OVERVIEWS=YES" , "COMPRESS=LZW", "PREDICTOR=2", "TILED=YES", fileIndicatorSource_tmp, fileIndicatorDestination)
            print(gdal_calc_process)
            os.system(gdal_calc_process)   
            os.remove(fileIndicatorSource_tmp)           
            annual_delta_NBR_vrt.write(fileIndicatorDestination + "\n")        
            sql = 'UPDATE scarichi2 SET "path_annualdeltaNBR" = %s WHERE (EXTRACT(MONTH FROM data_acq) = %s AND EXTRACT(YEAR FROM data_acq) = %s AND tile = %s)' 
            sqldata = (fileIndicatorDestination, month, year, tile)  
            cur.execute(sql, sqldata)    
            print("query: " + cur.query)            
            conn.commit()         
                              
            annual_delta_NBR_vrt.close()
        else:
            print("files soure A and B are NOT both available")  
        
        
####################################################################
# creation file vrt using the txt file filled with the series of tif images compose the vrt
####################################################################       

print("------------------------------------------------------") 
print("month_year_available len: " + str(len(month_year_available)))
print("month_year_available : " + str(month_year_available))   
month_year_available = sorted(month_year_available, key=lambda tup: tup[1]+tup[0]) 
print("month_year_available sorted: " + str(month_year_available)) 


for month_year in month_year_available: 
    month, year = month_year
    ########################################################################
    # Generate VRT for annual_delta_NBR
    ########################################################################
    folderFileListForVrt = pathSaveFinalImageBase + year +"/"+ month
    print("Generate VRT for annual_delta_NBR.....................................")
    gdal_calc_str = '{0} -input_file_list {1} -overwrite {2}'
    vrt_list_files = folderFileListForVrt + "/annual_delta_NBR_" + year + "_" + month + ".txt"
    vrt_output_file = folderFileListForVrt + "/annual_delta_NBR_" + year + "_" + month + ".vrt"
    gdal_calc_process = gdal_calc_str.format(gdal_build_vrt_path, vrt_list_files, vrt_output_file)
    print(gdal_calc_process)
    ret = os.system(gdal_calc_process)    
    print("\n\nRET RET RET " + str(ret))    
    
    ################################################# TMP TMP #################################################
    datetime_threshold = datetime.datetime(year_before_inc_file_already_available, month_before_inc_file_already_available, 1)
    datetime_data = datetime.datetime(int(year), int(month),1)
    if(datetime_data>=datetime_threshold): # questo per evitare di andare a ricreare nuovamente se vado a rielaborare dati gia fatti !!!!!!!!
    ###if(year>=year_before_inc_file_already_available and month >= month_before_inc_file_already_available): # questo per evitare di andare a ricreare nuovamente se vado a rielaborare dati gia fatti !!!!!!!!
    ################################################# TMP TMP #################################################    
        ####################################################################
        # generate file .inc and fill its for annual_delta_NBR
        #################################################################### 
        createFolder(include_ogc_path + '/annual_delta_NBR/' + year)
        
        annual_delta_NBR_wms = open(include_ogc_path + '/annual_delta_NBR/' + year + "/wms_" + str(month).zfill(2) + ".inc" , 'w')        
        annual_delta_NBR_wms_template = template_wms        
        annual_delta_NBR_wms_template = annual_delta_NBR_wms_template.replace("$INDICE$","annual_delta_NBR")
        annual_delta_NBR_wms_template = annual_delta_NBR_wms_template.replace("$ANNO$",str(year))
        annual_delta_NBR_wms_template = annual_delta_NBR_wms_template.replace("$MESE$",str(month))
        annual_delta_NBR_wms_template = annual_delta_NBR_wms_template.replace("$VRT_PATH$", folderFileListForVrt + "/annual_delta_NBR_" + year + "_" + month + ".vrt")   
        annual_delta_NBR_wms_template = annual_delta_NBR_wms_template.replace("$BANDS$","1")
        annual_delta_NBR_wms_template = annual_delta_NBR_wms_template.replace("$PROCESSING$","SCALE=-11000,11000")#"SCALE=AUTO")
        annual_delta_NBR_wms.write(annual_delta_NBR_wms_template)
        annual_delta_NBR_wms.close()
        print("static_mapfile_wms_annual_delta_NBR: " + static_mapfile_wms_annual_delta_NBR)
        mode = 'a' if os.path.exists(static_mapfile_wms_annual_delta_NBR) else 'w'
        mapfile_wms_annual_delta_NBR_file = open(static_mapfile_wms_annual_delta_NBR , mode)  
        mapfile_wms_annual_delta_NBR_string = 'INCLUDE "' + str(include_ogc_path) + "/annual_delta_NBR/" + str(year) + "/wms_" + str(month).zfill(2) + '.inc"\n'
        mapfile_wms_annual_delta_NBR_file.write(mapfile_wms_annual_delta_NBR_string)
        mapfile_wms_annual_delta_NBR_file.close()
            
        annual_delta_NBR_wcs = open(include_ogc_path + '/annual_delta_NBR/' + year + "/wcs_" + str(month).zfill(2) + ".inc" , 'w')
        annual_delta_NBR_wcs_template = template_wcs        
        annual_delta_NBR_wcs_template = annual_delta_NBR_wcs_template.replace("$INDICE$","annual_delta_NBR")
        annual_delta_NBR_wcs_template = annual_delta_NBR_wcs_template.replace("$ANNO$",str(year))
        annual_delta_NBR_wcs_template = annual_delta_NBR_wcs_template.replace("$MESE$",str(month))
        annual_delta_NBR_wcs_template = annual_delta_NBR_wcs_template.replace("$VRT_PATH$", folderFileListForVrt + "/annual_delta_NBR_" + year + "_" + month + ".vrt")                
        annual_delta_NBR_wcs.write(annual_delta_NBR_wcs_template)         
        annual_delta_NBR_wcs.close()    
        print("static_mapfile_wcs_annual_delta_NBR: " + static_mapfile_wcs_annual_delta_NBR)
        mode = 'a' if os.path.exists(static_mapfile_wcs_annual_delta_NBR) else 'w'
        mapfile_wcs_annual_delta_NBR_file = open(static_mapfile_wcs_annual_delta_NBR , mode)     
        mapfile_wcs_annual_delta_NBR_string = 'INCLUDE "' + str(include_ogc_path) + "/annual_delta_NBR/" + str(year) + "/wcs_" + str(month).zfill(2) + '.inc"\n'
        mapfile_wcs_annual_delta_NBR_file.write(mapfile_wcs_annual_delta_NBR_string)
        mapfile_wcs_annual_delta_NBR_file.close()
        
   
print("------------------------------------------------------")       
print("------------------------------------------------------") 
print("") 
cur.close()
conn.close()

logDB.scrivi(str(nr_files_moved) + " annual_delta_NBR files created and moved", "")
logDB.chiudi(True)
log_msg("organize annual_delta_NBR procedure terminated " + str(nr_files_moved) + " files created and moved")
