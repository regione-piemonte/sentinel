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
from csi_sentinel_config import *
from csi_sentinel_log import *


def sizeof_fmt(num, suffix='B'):
    ''' by Fred Cirera,  https://stackoverflow.com/a/1094933/1870254, modified'''
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f %s%s" % (num, 'Yi', suffix)
#########################################################################################
def convertToSeconds(sec):
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    return "%d:%02d:%02d" % (h, m, s)
#########################################################################################
print("a")
t_0 = datetime.datetime.now()
session = requests.Session()
session.auth = (CSI_user, CSI_password)


printStartScript("starting elaboration.....")
logDB.scrivi("in execution", "")
# open the DB where the information for the download are stored
try:
    conn = psycopg2.connect(dbname=dbname, user=dbuser, host=dbhost, port=dbport, password=dbpass)
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
except:
    print ("ERROR: error in the database connection")                                                                                                 
#sql = "SELECT * FROM scarichi2 WHERE (data_elaboration_start IS NULL and path_images_extracted IS NOT NULL and (path_images_extracted LIKE '%T32TLQ%') and (title LIKE '%201909%'))"
sql = "SELECT * FROM scarichi2 WHERE (data_elaboration_end IS NULL and path_images_extracted IS NOT NULL) ORDER BY data_acq DESC LIMIT " + str(nr_processing_block)
dict_cur.execute(sql)

entries = []
for row in dict_cur:
    entries.append(row)
dict_cur.close()
cur = conn.cursor()
for row in entries:
    uuid = row["uuid_prod"]
    patch_images_available = row["path_images_extracted"]
    counter_slash = patch_images_available.count('/')
    path_img_10m = patch_images_available + "R10m/"
    path_img_20m = patch_images_available + "R20m/"
    path_img_60m = patch_images_available + "R60m/"
    patch_images_available_list_folder = path_img_10m.split("/")  
    year = patch_images_available_list_folder[counter_slash-7]#4
    month = patch_images_available_list_folder[counter_slash-6]#5
    tile =  patch_images_available_list_folder[counter_slash-5]#6
    print(str(counter_slash-4))
    data = patch_images_available_list_folder[counter_slash-4].split("_")[2][0:8]#7        
    year_ = data[0:4]
    month_ = data[4:6]
    day = data[6:8]
    time = patch_images_available_list_folder[counter_slash-4].split("_")[2][9:16]#7  
    fmt = '%Y.%m.%d'
    s = str(year_) + "." + str(month_) + "." + str(day)
    dt = datetime.datetime.strptime(s, fmt)
    tt = dt.timetuple() 
    dayoftheyear_images = tt.tm_yday    
    print("Those data are for dayoftheyear_images: " + str(dayoftheyear_images) + " - dt: " + str(dt))
    path_img_OUT = pathUnzipBase + year + "/" + month + "/" + tile + "/"    # fabio 25/08/2021 - path_img_OUT = pathUnzipBase + "/" + year + "/" + month + "/" + tile + "/"

    
    # update data_elaboration_start    
    tz = pytz.timezone('Europe/Rome')
    elaboration_time_start = datetime.datetime.now(tz)
    sql = "UPDATE scarichi2 SET data_elaboration_start = %s WHERE uuid_prod = %s"
    sqldata = (elaboration_time_start, uuid)
    cur.execute(sql, sqldata)                
    conn.commit()

    # settings argument
    gdal_calc_path  = '/usr/local/bin/gdal_calc.py'
    gdal_translate_path  = '/usr/local/bin/gdal_translate' 
    
    # the name of the extracted filename can be different in structure (previous March 2018  the data file star with "L2A_" prefix) 
    # we do not want to delete the prefix but we want be able to understand when the format filename is not the "classic" (this happend
    # if the file with usual filename is not present and in this case we will use the formulation with "L2A_" prefix)    
    src_file_B02 = path_img_10m + tile + "_" +  year + month + day + "T" + str(time) + "_B02_10m.jp2"  
    if not os.path.isfile(src_file_B02):
        src_file_B02 = path_img_10m + "L2A_" + tile + "_" +  year + month + day + "T" + str(time) + "_B02_10m.jp2"
        
    src_file_B03 = path_img_10m + tile + "_" +  year + month + day + "T" + str(time) + "_B03_10m.jp2"  
    if not os.path.isfile(src_file_B03):     
        src_file_B03 = path_img_10m +  "L2A_" + tile + "_" +  year + month + day + "T" + str(time) + "_B03_10m.jp2"
        
    src_file_B04 = path_img_10m + tile + "_" +  year + month + day + "T" + str(time) + "_B04_10m.jp2"
    if not os.path.isfile(src_file_B04):
        src_file_B04 = path_img_10m +  "L2A_" + tile + "_" +  year + month + day + "T" + str(time) + "_B04_10m.jp2"
        
    src_file_B08 = path_img_10m + tile + "_" +  year + month + day + "T" + str(time) + "_B08_10m.jp2"
    if not os.path.isfile(src_file_B08):
        src_file_B08 = path_img_10m +  "L2A_" + tile + "_" +  year + month + day + "T" + str(time) + "_B08_10m.jp2"
        
    src_file_B11 = path_img_20m + tile + "_" +  year + month + day + "T" + str(time) + "_B11_20m.jp2"
    if not os.path.isfile(src_file_B11):
        src_file_B11 = path_img_20m +  "L2A_" + tile + "_" +  year + month + day + "T" + str(time) + "_B11_20m.jp2"
        
    src_file_B12 = path_img_20m + tile + "_" +  year + month + day + "T" + str(time) + "_B12_20m.jp2"
    if not os.path.isfile(src_file_B12):
        src_file_B12 = path_img_20m +  "L2A_" + tile + "_" +  year + month + day + "T" + str(time) + "_B12_20m.jp2"
    
    src_file_SCL_20m = path_img_20m + tile + "_" +  year + month + day + "T" + str(time) + "_SCL_20m.jp2"
    src_file_B11_10m = path_img_10m + tile + "_" +  year + month + day + "T" + str(time) + "_B11_10m_derived.jp2"
    src_file_B12_10m = path_img_10m + tile + "_" +  year + month + day + "T" + str(time) + "_B12_10m_derived.jp2"
    src_file_SCL_10m = path_img_10m + tile + "_" +  year + month + day + "T" + str(time) + "_SCL_10m_derived.jp2"    
    if not os.path.isfile(src_file_SCL_20m):
        src_file_SCL_20m = path_img_20m +  "L2A_" + tile + "_" +  year + month + day + "T" + str(time) + "_SCL_20m.jp2"
        src_file_B11_10m = path_img_10m +  "L2A_" + tile + "_" +  year + month + day + "T" + str(time) + "_B11_10m_derived.jp2"
        src_file_B12_10m = path_img_10m +  "L2A_" + tile + "_" +  year + month + day + "T" + str(time) + "_B12_10m_derived.jp2"
        src_file_SCL_10m = path_img_10m +  "L2A_" + tile + "_" +  year + month + day + "T" + str(time) + "_SCL_10m_derived.jp2"         

    
    # setting output files
    file_risultati_NDVI = path_img_OUT + "tmp_result_NDVI_" + tile + "_" + year + "_" + month + ".tif" 
    file_final_NDVI = path_img_OUT + "NDVI_" + tile + "_" + year + "_" + month + ".tif"
    file_final_mask_NDVI = path_img_OUT + "mask_" + tile + "_" + year + "_" + month + ".tif"    
    file_risultati_EVI = path_img_OUT + "tmp_result_EVI_" + tile + "_" + year + "_" + month + ".tif"
    file_final_EVI = path_img_OUT + "EVI_" + tile + "_" + year + "_" + month + ".tif"
#    file_final_mask_EVI = path_img_OUT + "mask_EVI_" + tile + "_" + year + "_" + month + ".tif"     
    file_risultati_NBR = path_img_OUT + "tmp_result_NBR_" + tile + "_" + year + "_" + month + ".tif"
    file_final_NBR = path_img_OUT + "NBR_" + tile + "_" + year + "_" + month + ".tif"
#    file_final_mask_NBR = path_img_OUT + "mask_NBR_" + tile + "_" + year + "_" + month + ".tif"   
    file_risultati_NDWI = path_img_OUT + "tmp_result_NDWI_" + tile + "_" + year + "_" + month + ".tif"
    file_final_NDWI = path_img_OUT + "NDWI_" + tile + "_" + year + "_" + month + ".tif"
#    file_final_mask_NDWI = path_img_OUT + "mask_NDWI_" + tile + "_" + year + "_" + month + ".tif"
    file_final_cloudiness = path_img_OUT + "cloudiness_" + tile + "_" + year + "_" + month + ".tif"
    file_final_mask_cloudness = path_img_OUT + "cloudiness_mask_" + tile + "_" + year + "_" + month + ".tif"
    
    # Setting formulas to applay. A = B02, B = B04, C = B08, D = B11, E = B12, F = B03   
    nodata = '-32000'
    nodataEVI = '-99999'         
    calc_expr_NDVI = '"((((C*1.0-B*1.0)/(C*1.0+B*1.0)) * 10000) * (logical_and(C!=0,B!=0)))  + (-32000 * logical_or(C==0, B==0))"'
    calc_expr_EVI = '"(((C*1.0-B*1.0)/((C*1.0 + (6.0*B*1.0) - (7.5*A*1.0) + 1.0) * 1.0)) * 2.5 * 10000 * (logical_and(C!=0,B!=0,A!=0))) + (-99999 * logical_or(C==0, B==0, A==0))"'
    calc_expr_NBR = '"((((C*1.0-E*1.0)/(C*1.0+E*1.0)) * 10000) * (logical_and(C!=0,E!=0)))  + (-32000 * logical_or(C==0, E==0))"'
    calc_expr_NDWI = '"((((F*1.0-D*1.0)/(F*1.0+D*1.0)) * 10000) * (logical_and(F!=0,D!=0)))  + (-32000 * logical_or(F==0, D==0))"'   
    typeof = '"Int16"'
    typeofEVI = '"Int32"'
    
       
    ########################################################################################################################################
    ########################################################################
    sql = "UPDATE scarichi2 SET processing_step = %s WHERE uuid_prod = %s"
    sqldata = ("Creation B11_10m and B12_10m, step (0/7) ", uuid)
    cur.execute(sql, sqldata)                
    conn.commit()
    ########################################################################      
    ########################################################################
    # Change image resolution for B11, B12 and SCL available at 20 meters resolution (up-scaling to 10 meters)
    ########################################################################
    print("Generate B11 jp2 at 10m image resolution")
    gdal_calc_str = '{0} -of {1} -outsize {2} {3} -r {4} {5} {6}'
    gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff" , "10980", "10980", "nearest", src_file_B11, src_file_B11_10m)
    print(gdal_calc_process)
    os.system(gdal_calc_process)     
    print("Generate B12 jp2 at 10m image resolution")
    gdal_calc_str = '{0} -of {1} -outsize {2} {3} -r {4} {5} {6}'
    gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff" , "10980",  "10980", "nearest", src_file_B12, src_file_B12_10m)
    os.system(gdal_calc_process)
    print(gdal_calc_process)
    print("Generate SCL jp2 at 10m image resolution")
    gdal_calc_str = '{0} -of {1} -outsize {2} {3} -r {4} {5} {6}'
    gdal_calc_process = gdal_calc_str.format(gdal_translate_path, "GTiff" , "10980", "10980", "nearest", src_file_SCL_20m, src_file_SCL_10m)
    print(gdal_calc_process)
    os.system(gdal_calc_process) 
        
#    alternative mode -> /usr/local/bin/gdal_translate -of GTiff -outsize 10980 10980 -r lanczos /dati/sentinel/work/2019/11/T32TLQ/S2B_MSIL2A_20191118T102219_N0213_R065_T32TLQ_20191118T122325.SAFE/GRANULE/L2A_T32TLQ_A014105_20191118T102220/IMG_DATA/R20m/T32TLQ_20191118T102219_SCL_20m.jp2 /dati/sentinel/work/2019/11/T32TLQ/S2B_MSIL2A_20191118T102219_N0213_R065_T32TLQ_20191118T122325.SAFE/GRANULE/L2A_T32TLQ_A014105_20191118T102220/IMG_DATA/R20m/T32TLQ_20191118T102219_SCL_10m.jp2                   
    ########################################################################################################################################
    



    ########################################################################################################################################   
    ########################################################################
    sql = "UPDATE scarichi2 SET processing_step = %s WHERE uuid_prod = %s"
    sqldata = ("Calculate SD, step (1/7) ", uuid)
    cur.execute(sql, sqldata)                
    conn.commit()
    ########################################################################     
    ########################################################################
    # Generate calcuation of the standard deviation (SD) for the image red channel
    ########################################################################
    print("Calculate SD.....................................")   
    # read the band 4, red channel
    img_SD = gdal.Open(src_file_B04, gdal.GA_ReadOnly)
    SD_mtx_values = np.array(img_SD.GetRasterBand(1).ReadAsArray()) 
    n_row = img_SD.RasterYSize
    n_col = img_SD.RasterXSize  
    # get this information 
    originX, pixelWidth, xskew, originY, yskew, pixelHeight = img_SD.GetGeoTransform()
    
    SD_val = str(int(np.std(SD_mtx_values))) # the value of the standard deviation
    SD_img_min = str(int(np.amin(SD_mtx_values))) # the min value of the in th matrix (this to detect incomplete image)
    SD_img_max = str(int(np.amax(SD_mtx_values))) # the min value of the in th matrix (this to detect incomplete image)
    SD_img_count_novalue = str((SD_mtx_values<=0).sum())
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("SD_img_max: " + SD_img_max)
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++")
    sql = "UPDATE scarichi2 SET sd_red_channel = %s WHERE uuid_prod = %s"                                
    sqldata = (SD_val, uuid)
    cur.execute(sql, sqldata) 
    sql = "UPDATE scarichi2 SET novalue_red_channel = %s WHERE uuid_prod = %s"
    sqldata = (SD_img_count_novalue, uuid)
    cur.execute(sql, sqldata)               
    conn.commit()
    
    img_SD = None     
    SD_mtx_values = None
    del SD_mtx_values         
    ########################################################################################################################################    
    



    ########################################################################################################################################   
    ########################################################################
    sql = "UPDATE scarichi2 SET processing_step = %s WHERE uuid_prod = %s"
    sqldata = ("Creation cloudiness, step (2/7) ", uuid)
    cur.execute(sql, sqldata)    
    print("debug " + str(0)) # debug print            
    conn.commit()
    ########################################################################     
    ########################################################################
    # Generate string of process cloudiness
    ######################################################################## 
    print("Generate cloudiness.....................................")   
    print("debug " + str(1)) # debug print
    # read the SCL image 
    print("Open: " + src_file_SCL_10m)
    img_SCL_10m = gdal.Open(src_file_SCL_10m, gdal.GA_ReadOnly)
    SCL_mtx_values = np.array(img_SCL_10m.GetRasterBand(1).ReadAsArray()) 
    n_row = img_SCL_10m.RasterYSize
    n_col = img_SCL_10m.RasterXSize   
    # get this information 
    originX, pixelWidth, xskew, originY, yskew, pixelHeight = img_SCL_10m.GetGeoTransform()
    img_SCL_10m = None
    # check if the image where the month information cloudiness is already available
    file_values_exist = os.path.isfile(file_final_cloudiness)
    
    if (file_values_exist):
        print("debug " + str(2)) # debug print  
        img = gdal.Open(file_final_cloudiness, gdal.GA_ReadOnly)
        print(str(file_final_cloudiness))
        old_mtx_values_band1 = np.array(img.GetRasterBand(1).ReadAsArray())
        print("debug " + str(3)) # debug print          
        old_mtx_values_band2 = np.array(img.GetRasterBand(2).ReadAsArray())
        print("debug " + str(4)) # debug print  
        old_mtx_values_band3 = np.array(img.GetRasterBand(3).ReadAsArray())
        print("debug " + str(5)) # debug print  
        img = None  
    else:
        print("NO CLOUDINESS FILE ALREADY AVAILABLE, CREATE ONE INITIALIZED TO 0")  
        old_mtx_values_band1 = np.full((n_row, n_col), 0) 
        print("debug " + str(6)) # debug print  
        old_mtx_values_band2 = np.full((n_row, n_col), 0) 
        print("debug " + str(7)) # debug print  
        old_mtx_values_band3 = np.full((n_row, n_col), 0) 
        print("debug " + str(8)) # debug print                                                            
        # 
    for scl_class in cloudiness_class_bad:   
        print("debug cloudiness_class_bad" + str(cloudiness_class_bad)) # debug print  
        print("debug " + str(9)) # debug print    
        print("debug n_row " + str(n_row)) # debug print  
        print("debug n_col " + str(n_col)) # debug print                 
        print("debug scl_class " + str(scl_class)) # debug print     
        tmp_mtx_scl_class = np.full((n_row, n_col), int(scl_class)) 
        print("debug " + str('9a')) # debug print  
        is_present_current_class = np.equal(SCL_mtx_values, tmp_mtx_scl_class)
        print("debug " + str('9b')) # debug print    
        is_present_current_class = is_present_current_class.astype(int) 
        print("debug " + str('9c')) # debug print   
        old_mtx_values_band1 = old_mtx_values_band1 + is_present_current_class
        print("debug " + str('9d')) # debug print  
      
    for scl_class in cloudiness_class_good:       
        print("debug " + str(10)) # debug print           
        tmp_mtx_scl_class = np.full((n_row, n_col), int(scl_class))      
        is_present_current_class = np.equal(SCL_mtx_values, tmp_mtx_scl_class)
        is_present_current_class = is_present_current_class.astype(int) 
        old_mtx_values_band2 = old_mtx_values_band2 + is_present_current_class     
    
    for scl_class in cloudiness_class_nodata: 
        print("debug " + str(11)) # debug print                 
        tmp_mtx_scl_class = np.full((n_row, n_col), int(scl_class))      
        is_present_current_class = np.equal(SCL_mtx_values, tmp_mtx_scl_class)
        is_present_current_class = is_present_current_class.astype(int) 
        old_mtx_values_band3 = old_mtx_values_band3 + is_present_current_class             
    tmp_mtx_scl_class = None
    del tmp_mtx_scl_class 
    print("debug " + str(12)) # debug print  
        
    driver = gdal.GetDriverByName('GTiff')
    print("debug " + str(13)) # debug print      
    outRaster = driver.Create(file_final_cloudiness, n_col, n_row, 3, gdal.GDT_Byte) 
    print("debug " + str(14)) # debug print      
    outRaster.SetGeoTransform((originX, pixelWidth, xskew, originY, yskew, pixelHeight))    
    print("debug " + str(15)) # debug print      
    outband1 = outRaster.GetRasterBand(1)
    print("debug " + str(16)) # debug print      
    outband1.WriteArray(old_mtx_values_band1)   
    print("debug " + str(17)) # debug print      
    outband2 = outRaster.GetRasterBand(2)
    print("debug " + str(18)) # debug print      
    outband2.WriteArray(old_mtx_values_band2)   
    print("debug " + str(19)) # debug print      
    outband3 = outRaster.GetRasterBand(3)
    print("debug " + str(20)) # debug print      
    outband3.WriteArray(old_mtx_values_band3)  
    print("debug " + str(21)) # debug print      
    outRasterSRS = osr.SpatialReference()
    print("debug " + str(22)) # debug print      
    outRasterSRS.ImportFromEPSG(32632)
    print("debug " + str(23)) # debug print      
    outRaster.SetProjection(outRasterSRS.ExportToWkt())
    print("debug " + str(24)) # debug print      
    
    outband1.FlushCache()
    outband2.FlushCache()
    outband3.FlushCache()
    print("debug " + str(25)) # debug print      
    outband1 = None
    outband2 = None
    outband3 = None
    outRaster = None
    print("debug " + str(26)) # debug print      
        
    SCL_mtx_values = None
    del SCL_mtx_values    
    print("debug " + str(27)) # debug print
    old_mtx_values_band1 = None
    del old_mtx_values_band1   
    print("debug " + str(28)) # debug print
    old_mtx_values_band2 = None
    del old_mtx_values_band2 
    print("debug " + str(29)) # debug print
    old_mtx_values_band3 = None
    del old_mtx_values_band3 
    print("debug " + str(30)) # debug print
    is_present_current_class  = None
    del is_present_current_class
    ########################################################################################################################################    
    
    
    
    ########################################################################################################################################      
    ########################################################################
    sql = "UPDATE scarichi2 SET processing_step = %s WHERE uuid_prod = %s"
    sqldata = ("Creation NDVI, step (3/7) ", uuid)
    cur.execute(sql, sqldata)                
    conn.commit()
    ########################################################################    
    ########################################################################
    # Generate string of process NDVI
    ########################################################################
    print("Generate NDVI.....................................")
    #gdal_calc_str = '/usr/bin/./python {0} -B {1} -C {2} --outfile={3} --calc={4} --NoDataValue={5} --type={6} --overwrite --debug'
    gdal_calc_str = '{0} -B {1} -C {2} --outfile={3} --calc={4} --NoDataValue={5} --type={6} --overwrite --debug ' 
    gdal_calc_process = gdal_calc_str.format(gdal_calc_path, src_file_B04, src_file_B08, file_risultati_NDVI, calc_expr_NDVI, nodata, typeof)
    print(gdal_calc_process)
    ret = os.system(gdal_calc_process)    
    print("\n\nRET RET RET " + str(ret))                                 
    #open the image just created with "gdal_calc.py" and store in tmp_mtx_values
    img = gdal.Open(file_risultati_NDVI)    
    tmp_mtx_values = np.array(img.GetRasterBand(1).ReadAsArray()) 
        # ?? tolta 1/3/2021 tmp_mtx_values[tmp_mtx_values == 0] = int(nodata) # 2019-12-11 - 2019-12-11 - 2019-12-11 - 2019-12-11         ???                
    n_row = img.RasterYSize
    n_col = img.RasterXSize
    # get this information 
    originX, pixelWidth, xskew, originY, yskew, pixelHeight = img.GetGeoTransform()
    img = None
    
    # create a mask for the current image just created -> "tmp_mtx_mask". 
    # The matrix is initialized to the value "dayoftheyear_images"
    tmp_mtx_mask = np.full((n_row, n_col), dayoftheyear_images)
        
    img = gdal.Open(src_file_SCL_10m)    
    tmp_mtx_mask_cloudness = np.array(img.GetRasterBand(1).ReadAsArray()) 
    img = None    
    
    
    ##########################################################################################
    ############# AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA########        
    ##########################################################################################      
        
    # creo una matrice con 1 se il valore di SCL appartiene alla lista 'cloudiness_class_good'
    # e con 0 altrimenti (questo vuol dire 0 quando appartiene alle liste 'cloudiness_class_bad' o 'cloudiness_class_nodata')
       
    SCL_data_validity = np.full((n_row, n_col), 0) 
    for scl_class in cloudiness_class_good:
        tmp_mtx_scl_class = np.full((n_row, n_col), int(scl_class))      
        is_present_current_class = np.equal(tmp_mtx_mask_cloudness, tmp_mtx_scl_class)
        SCL_data_validity = np.logical_or(SCL_data_validity, is_present_current_class)   
    SCL_data_validity = SCL_data_validity.astype(int)      
            
    tmp_mtx_scl_class = None
    del tmp_mtx_scl_class 
    is_present_current_class = None
    del is_present_current_class

    # ho creato una matrice dove 1 se il valore da considerare, 0 altrimenti e quindi tengo vecchio valore
            
    ##########################################################################################
    ########################################################################################## 
    
        
    for name, size in sorted(((name, sys.getsizeof(value)) for name, value in locals().items()),key= lambda x: -x[1])[:10]:
        print("{:>30}: {:>8}".format(name, sizeof_fmt(size)))
                                
    # if is not present this is the first time (start of the month)
    file_values_exist = os.path.isfile(file_final_NDVI)
    file_mask_exist = os.path.isfile(file_final_mask_NDVI)
    file_mask_cloudness_exist = os.path.isfile(file_final_mask_cloudness)
    if (file_values_exist and file_mask_exist and file_mask_cloudness_exist):
        # the images are available and will be also apened
        img = gdal.Open(file_final_NDVI, gdal.GA_ReadOnly)
        old_mtx_values = np.array(img.GetRasterBand(1).ReadAsArray())     
        img = None                                
        img_mask = gdal.Open(file_final_mask_NDVI, gdal.GA_ReadOnly)
        old_mtx_mask = np.array(img_mask.GetRasterBand(1).ReadAsArray()) 
        img_mask = None        
        img_mask_cloudness = gdal.Open(file_final_mask_cloudness, gdal.GA_ReadOnly)
        old_mtx_mask_cloudness = np.array(img_mask_cloudness.GetRasterBand(1).ReadAsArray()) 
        img_mask_cloudness = None 
                    
        # matrix with the max from the old (from loaded file) matrix and the new one just created (new NDVI)
        #tmp_mtx_values[tmp_mtx_values == 0] = int(nodata) # 2019-12-11 - 2019-12-11 - 2019-12-11 - 2019-12-11
        out_mtx_values = np.maximum(old_mtx_values, tmp_mtx_values)  
        
        ##########################################################################################
        ############# AGGIUNTA2     
        ##########################################################################################           
        nodata_mtx = np.full((n_row, n_col), int(nodata))      
        out_mtx_values = np.maximum(old_mtx_values, np.add(np.multiply(SCL_data_validity,tmp_mtx_values), np.multiply(np.logical_not(SCL_data_validity),nodata_mtx)))
        nodata_mtx = None
        del nodata_mtx  
        ##########################################################################################
        ############# AGGIUNTA2     
        ##########################################################################################   
        
        
        # this tmp_mtx_mask is initialized with the value doy of the data_acq. Is used the firt time before history image are availablr
        # in this loop is not nedeed, also we can delete it
        tmp_mtx_mask = None
        del tmp_mtx_mask         
                
        for name, size in sorted(((name, sys.getsizeof(value)) for name, value in locals().items()),key= lambda x: -x[1])[:10]:
            print("{:>30}: {:>8}".format(name, sizeof_fmt(size)))           
        print(" -----------------------------------------------------------")
        
        
        # mask with 1 if the max  pixel value arrived from tmp_mtx_values (current) and the same is NOT the same in old_mtx_values.
        # The one already present(old) is less then the value calculated now.
                                                        
        
        out_mtx_mask_A_from_tmp_mtx_values = np.equal(tmp_mtx_values, out_mtx_values)   # 1 where the vale came from the implemented funtion (now), zero others
        
        ##########################################################################################
        ############# AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA########        
        ##########################################################################################        
        # out_mtx_mask_A_from_tmp_mtx_values  1 se valore maggiore e quello attuale e se la matrice dei valori di SCL buoni e ad 1    #(riga vecchia sopra eliminabile)
        nodata_mtx = np.full((n_row, n_col), int(nodata))         
        out_mtx_mask_A_from_tmp_mtx_values = np.equal(np.add(np.multiply(SCL_data_validity,tmp_mtx_values), np.multiply(np.logical_not(SCL_data_validity),nodata_mtx)), out_mtx_values)
        nodata_mtx = None
        del nodata_mtx          
        ##########################################################################################
                  
        
        
        out_mtx_mask_A_from_old_mtx_values = np.equal(old_mtx_values, out_mtx_values)   # 1 where the vale came from the old file loaded (past), zero others
                
        ##########################################################################################
        ############# AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA########        
        ##########################################################################################        
        # out_mtx_mask_A_from_old_mtx_values e 1 se valore maggiore era il precedente o comunque il valore attuale non e buono avendo SCL a 0     #(riga vecchia sopra eliminabile)             
##        out_mtx_mask_A_from_old_mtx_values = np.logical_or(np.equal(old_mtx_values, out_mtx_values),  np.logical_not(SCL_data_validity))
        ##########################################################################################
        ##########################################################################################           
        
        old_mtx_values = None
        del old_mtx_values
        tmp_mtx_values = None
        del tmp_mtx_values          
        out_mtx_mask_A = np.logical_and(out_mtx_mask_A_from_tmp_mtx_values, np.logical_not(out_mtx_mask_A_from_old_mtx_values))  # this to tae care when the new calculated value is a max and is equal to the old max value                        
        out_mtx_mask_A_from_tmp_mtx_values = None
        del out_mtx_mask_A_from_tmp_mtx_values                
        out_mtx_mask_A_filtering = out_mtx_mask_A.astype(int) # convert as an integer value (the values in this matrix will be 0 or 1)
        out_mtx_mask_A = out_mtx_mask_A_filtering * dayoftheyear_images # in all the 1 of the matrix we will write the "dayoftheyear_images" value
        out_mtx_mask_C =  np.multiply(out_mtx_mask_A_filtering, tmp_mtx_mask_cloudness)
        # out_mtx_mask_A now will contain the value of the day (acq day of the current image) in the position where there is the max NDVI (0 in the other place)
        # out_mtx_mask_C now will contain the value of the cloudness from the current image in the position where there is the max NDVI (0 in the other place) 
        out_mtx_mask_A_filtering = None
        del out_mtx_mask_A_filtering
        # now out_mtx_mask_A will be zero except the value where the max NDVI is comming from the current elaborted image. In those points will be the doy of acq day
        # now out_mtx_mask_B will be zero except the value where the max NDVI is comming from the current elaborted image. In those points will be the class SCL of the day in that point
                              
        out_mtx_mask_B_filtering = out_mtx_mask_A_from_old_mtx_values.astype(int)  # 1 where the vale came from the old file loaded (past), zero others
        out_mtx_mask_A_from_old_mtx_values = None
        del out_mtx_mask_A_from_old_mtx_values
        out_mtx_mask_B = np.multiply(out_mtx_mask_B_filtering, old_mtx_mask) # get the old value in the mask where 1
        old_mtx_mask = None
        del old_mtx_mask          
        out_mtx_mask_D = np.multiply(out_mtx_mask_B_filtering, old_mtx_mask_cloudness) # get the old value in the cloudness where 1
        old_mtx_mask_cloudness = None
        del old_mtx_mask_cloudness 
        out_mtx_mask_B_filtering = None
        del out_mtx_mask_B_filtering
                      
        out_mtx_mask = out_mtx_mask_A + out_mtx_mask_B   # sum old and new for the mask 
        tmp_mtx_mask_NDVI = out_mtx_mask            

        out_mtx_mask_A = None
        del out_mtx_mask_A        
        out_mtx_mask_B = None
        del out_mtx_mask_B            
        
        out_mtx_mask_cloudness = out_mtx_mask_C + out_mtx_mask_D # sum old and new for the cloudness     
        
        out_mtx_mask_C = None
        del out_mtx_mask_C
        out_mtx_mask_D = None
        del out_mtx_mask_D     
          
        ##########################################################################################
        ##########################################################################################
        
        for name, size in sorted(((name, sys.getsizeof(value)) for name, value in locals().items()),key= lambda x: -x[1])[:10]:
            print("{:>30}: {:>8}".format(name, sizeof_fmt(size)))
        
    else:
        print("NO FILES ALREADY AVAILABLE")
        ##########################################################################################
        ############# AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA########        
        ########################################################################################## 
    
        # se sono qui sono alla prima acqusizione del mese
        nodata_mtx = np.full((n_row, n_col), int(nodata))
        # a questo punto SCL_data_validity avro 1 se il valore e da buono, 0 alrimenti. Questo per la prima immagine  disponibile del mese 
        tmp_mtx_values = np.add(np.multiply(tmp_mtx_values, SCL_data_validity), np.multiply(nodata_mtx, np.logical_not(SCL_data_validity)))
        nodata_mtx = None
        del nodata_mtx   
        ##########################################################################################
        ############# AGGIUNTA2     
        ########################################################################################## 
        # se sono qui sono alla prima acqusizione del mese
        nodata_mtx = np.full((n_row, n_col), int(-1))
        # a questo punto SCL_data_validity avro 1 se il valore e da buono, 0 alrimenti. Questo per la prima immagine  disponibile del mese 
        tmp_mtx_mask = np.add(np.multiply(tmp_mtx_mask, SCL_data_validity), np.multiply(nodata_mtx, np.logical_not(SCL_data_validity)))
        nodata_mtx = None
        del nodata_mtx 
        
        # se sono qui sono alla prima acqusizione del mese
        nodata_mtx = np.full((n_row, n_col), int(0))
        # a questo punto SCL_data_validity avro 1 se il valore e da buono, 0 alrimenti. Questo per la prima immagine  disponibile del mese 
        tmp_mtx_mask_cloudness = np.add(np.multiply(tmp_mtx_mask_cloudness, SCL_data_validity), np.multiply(nodata_mtx, np.logical_not(SCL_data_validity)))
        nodata_mtx = None
        del nodata_mtx       
        ##########################################################################################
        ############# AGGIUNTA2     
        ##########################################################################################         
    
        ##########################################################################################
        ##########################################################################################        
        out_mtx_values = tmp_mtx_values # NDVI (valore di NDVI calcolato) # cancella commento
        out_mtx_mask = tmp_mtx_mask # mask_NDVI (in questo caso ha tutta valore del primo giorno del mese) # cancella commento
        out_mtx_mask_cloudness = tmp_mtx_mask_cloudness # mask_ccloudness (prima immagine di SCL) # cancella commento
        
    driver = gdal.GetDriverByName('GTiff')
    outRaster = driver.Create(file_final_NDVI, n_col, n_row, 1, gdal.GDT_Int16)  
    outRaster.SetGeoTransform((originX, pixelWidth, xskew, originY, yskew, pixelHeight))
    outband = outRaster.GetRasterBand(1)
    outband.WriteArray(out_mtx_values)
    outRasterSRS = osr.SpatialReference()
    outRasterSRS.ImportFromEPSG(32632)
    outRaster.SetProjection(outRasterSRS.ExportToWkt())
    outband.FlushCache()  
    outband = None
    outRaster = None
    
    driver1 = gdal.GetDriverByName('GTiff')
    outRaster1 = driver1.Create(file_final_mask_NDVI, n_col, n_row, 1, gdal.GDT_UInt16)  
    outRaster1.SetGeoTransform((originX, pixelWidth, xskew, originY, yskew, pixelHeight))
    outband1 = outRaster1.GetRasterBand(1)
    outband1.WriteArray(out_mtx_mask)   
    outRasterSRS1 = osr.SpatialReference()
    outRasterSRS1.ImportFromEPSG(32632)
    outRaster1.SetProjection(outRasterSRS1.ExportToWkt())
    outband1.FlushCache()
    outband1 = None
    outRaster1 = None
    
    driver1 = gdal.GetDriverByName('GTiff')
    outRaster1 = driver1.Create(file_final_mask_cloudness, n_col, n_row, 1, gdal.GDT_UInt16)  
    outRaster1.SetGeoTransform((originX, pixelWidth, xskew, originY, yskew, pixelHeight))
    outband1 = outRaster1.GetRasterBand(1)
    outband1.WriteArray(out_mtx_mask_cloudness)   
    outRasterSRS1 = osr.SpatialReference()
    outRasterSRS1.ImportFromEPSG(32632)
    outRaster1.SetProjection(outRasterSRS1.ExportToWkt())
    outband1.FlushCache()
    outband1 = None
    outRaster1 = None    
    
    os.remove(str(file_risultati_NDVI))         
          
    tmp_mtx_values = None
    del tmp_mtx_values
    tmp_mtx_mask = None
    del tmp_mtx_mask   
    tmp_mtx_mask_cloudness = None
    del tmp_mtx_mask_cloudness
    out_mtx_values = None
    del out_mtx_values
    out_mtx_mask = None
    del out_mtx_mask 
    out_mtx_mask_cloudness = None
    del out_mtx_mask_cloudness     
    
    ########################################################################################################################################


    ########################################################################################################################################                                                                  
    ########################################################################                                                                                                                                                                           
    sql = "UPDATE scarichi2 SET processing_step = %s WHERE uuid_prod = %s"
    sqldata = ("Creation EVI, step (4/7) ", uuid)
    cur.execute(sql, sqldata)                
    conn.commit()    
    ########################################################################      
    ########################################################################
    # Generate string of process EVI
    ########################################################################
    print("Generate EVI......................................")
    #gdal_calc_str = '/usr/bin/./python {0} -A {1} -B {2} -C {3} --outfile={4} --calc={5} --NoDataValue={6} --type={7} --debug'
    gdal_calc_str = '{0} -A {1} -B {2} -C {3} --outfile={4} --calc={5} --NoDataValue={6} --type={7} --debug'
    gdal_calc_process = gdal_calc_str.format(gdal_calc_path, src_file_B02, src_file_B04, src_file_B08, file_risultati_EVI, calc_expr_EVI, nodataEVI, typeofEVI)
    print(gdal_calc_process)
    os.system(gdal_calc_process)          
    print("\nRET RET RET " + str(ret))                                          
    #open the image just created with "gdal_calc.py" and store in tmp_mtx_values
    img = gdal.Open(file_risultati_EVI)
    tmp_mtx_values = np.array(img.GetRasterBand(1).ReadAsArray())                              
    n_row = img.RasterYSize
    n_col = img.RasterXSize
    # get this information 
    originX, pixelWidth, xskew, originY, yskew, pixelHeight = img.GetGeoTransform()
    img = None                      
    
    # if is not present this is the first time (start of the month)
    file_values_exist = os.path.isfile(file_final_EVI)
    if (file_values_exist):        
        img = gdal.Open(file_final_EVI, gdal.GA_ReadOnly)
        old_mtx_values = np.array(img.GetRasterBand(1).ReadAsArray())
        img = None
             
        # use NDVI mask for EVI 
        out_mtx_mask = tmp_mtx_mask_NDVI 
        out_mtx_A = np.equal(out_mtx_mask, dayoftheyear_images)
        
        ##########################################################################################
        ############# AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA########        
        ##########################################################################################
        out_mtx_A = np.logical_and(np.equal(out_mtx_mask, dayoftheyear_images), SCL_data_validity)   #(riga vecchia sopra eliminabile)         
        ##########################################################################################
        ##########################################################################################  
        
        out_mtx_A = out_mtx_A.astype(int)
        out_mtx_A = np.multiply(out_mtx_A, tmp_mtx_values)
        
        out_mtx_B = np.logical_not(np.equal(out_mtx_mask, dayoftheyear_images))
        
        ##########################################################################################
        ############# AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA########        
        ##########################################################################################        
##        out_mtx_B = np.logical_and(np.equal(out_mtx_mask, dayoftheyear_images), SCL_data_validity)   #(riga vecchia sopra eliminabile)  
        out_mtx_B = np.logical_not(np.logical_and(np.equal(out_mtx_mask, dayoftheyear_images), SCL_data_validity))
        ##########################################################################################
        ##########################################################################################    
        
        out_mtx_B = out_mtx_B.astype(int)
        out_mtx_B = np.multiply(out_mtx_B, old_mtx_values)
        
        out_mtx_values = out_mtx_A + out_mtx_B
        
        out_mtx_A = None
        del out_mtx_A
        out_mtx_B = None
        del out_mtx_B                               
        old_mtx_values = None
        del old_mtx_values

    else:
        print("NO FILES ALREADY AVAILABLE")
        ##########################################################################################
        ############# AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA########        
        ########################################################################################## 
    
        # se sono qui sono alla prima acqusizione del mese
        nodata_mtx = np.full((n_row, n_col), int(nodataEVI))
        # a questo punto SCL_data_validity avro 1 se il valore e da buono, 0 alrimenti. Questo per la prima immagine  disponibile del mese 
        ####fabio 02/09/2021 - tmp_mtx_values = np.add(np.multiply(tmp_mtx_values, SCL_data_validity), np.multiply(nodata_mtx, np.logical_not(SCL_data_validity)))
        out_mtx_values = np.add(np.multiply(tmp_mtx_values, SCL_data_validity), np.multiply(nodata_mtx, np.logical_not(SCL_data_validity)))
        nodata_mtx = None
        del nodata_mtx               
    
        ##########################################################################################
        ##########################################################################################  
        ####fabio 02/09/2021 - out_mtx_values = tmp_mtx_values
       
         
    driver = gdal.GetDriverByName('GTiff')
    outRaster = driver.Create(file_final_EVI, n_col, n_row, 1, gdal.GDT_Int32)  # GDT_Byte  GDT_Float32
    outRaster.SetGeoTransform((originX, pixelWidth, xskew, originY, yskew, pixelHeight))
    outband = outRaster.GetRasterBand(1)
    outband.WriteArray(out_mtx_values)
    outRasterSRS = osr.SpatialReference()
    outRasterSRS.ImportFromEPSG(32632)
    outRaster.SetProjection(outRasterSRS.ExportToWkt())
    outband.FlushCache()  
    outband = None
    outRaster = None
            
    os.remove(str(file_risultati_EVI))  
      
    tmp_mtx_values = None
    del tmp_mtx_values
    out_mtx_values = None
    del out_mtx_values
    out_mtx_mask = None
    del out_mtx_mask     
    ########################################################################################################################################                       


    
    ########################################################################################################################################                                               
    ########################################################################                                                                                                                                                                            
    sql = "UPDATE scarichi2 SET processing_step = %s WHERE uuid_prod = %s"
    sqldata = ("Creation NBR, step (5/7) ", uuid)
    cur.execute(sql, sqldata)                
    conn.commit()    
    ########################################################################      
    ########################################################################
    # Generate string of process NBR
    ########################################################################
    print("Generate NBR......................................")
    #gdal_calc_str = '/usr/bin/./python {0} -C {1} -E {2} --outfile={3} --calc={4} --NoDataValue={5} --type={6} --debug'
    gdal_calc_str = '{0} -C {1} -E {2} --outfile={3} --calc={4} --NoDataValue={5} --type={6} --debug'
    gdal_calc_process = gdal_calc_str.format(gdal_calc_path, src_file_B08, src_file_B12_10m, file_risultati_NBR, calc_expr_NBR, nodata, typeof)
    os.system(gdal_calc_process)                                           
    #open the image just created with "gdal_calc.py" and store in tmp_mtx_values
    img = gdal.Open(file_risultati_NBR)
    tmp_mtx_values = np.array(img.GetRasterBand(1).ReadAsArray())                          
    n_row = img.RasterYSize
    n_col = img.RasterXSize
    # get this information 
    originX, pixelWidth, xskew, originY, yskew, pixelHeight = img.GetGeoTransform()
    img = None                      
    
    # if is not present this is the first time (start f the month)
    file_values_exist = os.path.isfile(file_final_NBR)
    if (file_values_exist):
        img = gdal.Open(file_final_NBR, gdal.GA_ReadOnly)
        old_mtx_values = np.array(img.GetRasterBand(1).ReadAsArray())
        img = None
             
        # use NDVI mask for  NBR
        out_mtx_mask = tmp_mtx_mask_NDVI 
        out_mtx_A = np.equal(out_mtx_mask, dayoftheyear_images) # riga vecchia, eliminabile
        ##########################################################################################
        ############# AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA########        
        ##########################################################################################
        out_mtx_A = np.logical_and(np.equal(out_mtx_mask, dayoftheyear_images), SCL_data_validity)   
        ##########################################################################################
        ##########################################################################################    
        out_mtx_A = out_mtx_A.astype(int)
        out_mtx_A = np.multiply(out_mtx_A, tmp_mtx_values)
        
        out_mtx_B = np.logical_not(np.equal(out_mtx_mask, dayoftheyear_images)) # riga vecchia, eliminabile
        ##########################################################################################
        ############# AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA########        
        ##########################################################################################        
        out_mtx_B = np.logical_not(np.logical_and(np.equal(out_mtx_mask, dayoftheyear_images), SCL_data_validity))
        ##########################################################################################
        ########################################################################################## 
        out_mtx_B = out_mtx_B.astype(int)
        out_mtx_B = np.multiply(out_mtx_B, old_mtx_values)
        
        out_mtx_values = out_mtx_A + out_mtx_B
        
        out_mtx_A = None
        del out_mtx_A
        out_mtx_B = None
        del out_mtx_B                          
        old_mtx_values = None
        del old_mtx_values

    else:
        print("NO FILES ALREADY AVAILABLE")
        ##########################################################################################
        ############# AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA########        
        ########################################################################################## 
    
        # se sono qui sono alla prima acqusizione del mese
        nodata_mtx = np.full((n_row, n_col), int(nodata))
        # a questo punto SCL_data_validity avro 1 se il valore e da buono, 0 alrimenti. Questo per la prima immagine  disponibile del mese 
        ####fabio 02/09/2021 - tmp_mtx_values = np.add(np.multiply(tmp_mtx_values, SCL_data_validity), np.multiply(nodata_mtx, np.logical_not(SCL_data_validity)))
        out_mtx_values = np.add(np.multiply(tmp_mtx_values, SCL_data_validity), np.multiply(nodata_mtx, np.logical_not(SCL_data_validity)))
        nodata_mtx = None
        del nodata_mtx           
    
        ##########################################################################################
        ##########################################################################################        
        ####fabio 02/09/2021 - out_mtx_values = tmp_mtx_values
     
    
    driver = gdal.GetDriverByName('GTiff')
    outRaster = driver.Create(file_final_NBR, n_col, n_row, 1, gdal.GDT_Int16)  # GDT_Byte  GDT_Float32
    outRaster.SetGeoTransform((originX, pixelWidth, xskew, originY, yskew, pixelHeight))
    outband = outRaster.GetRasterBand(1)
    outband.WriteArray(out_mtx_values)
    outRasterSRS = osr.SpatialReference()
    outRasterSRS.ImportFromEPSG(32632)
    outRaster.SetProjection(outRasterSRS.ExportToWkt())
    outband.FlushCache()  
    outband = None
    outRaster = None
            
    os.remove(str(file_risultati_NBR))  
      
    tmp_mtx_values = None
    del tmp_mtx_values       
    out_mtx_values = None
    del out_mtx_values
    out_mtx_mask = None
    del out_mtx_mask         
    ########################################################################################################################################


    
    ########################################################################################################################################
    ########################################################################                                                                                                                                                                                                                               
    sql = "UPDATE scarichi2 SET processing_step = %s WHERE uuid_prod = %s"
    sqldata = ("Creation NDWI, step (6/7) ", uuid)
    cur.execute(sql, sqldata)                
    conn.commit()        
    ########################################################################    
    ########################################################################
    # Generate string of process NDWI
    ########################################################################
    print("Generate NDWI......................................")
    #gdal_calc_str = '/usr/bin/./python {0} -F {1} -D {2} --outfile={3} --calc={4} --NoDataValue={5} --type={6} --debug'
    gdal_calc_str = '{0} -F {1} -D {2} --outfile={3} --calc={4} --NoDataValue={5} --type={6} --debug'
    gdal_calc_process = gdal_calc_str.format(gdal_calc_path, src_file_B03, src_file_B11_10m, file_risultati_NDWI, calc_expr_NDWI, nodata, typeof)
    os.system(gdal_calc_process)                                           
    #open the image just created with "gdal_calc.py" and store in tmp_mtx_values
    img = gdal.Open(file_risultati_NDWI)
    tmp_mtx_values = np.array(img.GetRasterBand(1).ReadAsArray())                        
    n_row = img.RasterYSize
    n_col = img.RasterXSize
    # get this information 
    originX, pixelWidth, xskew, originY, yskew, pixelHeight = img.GetGeoTransform()
    img = None                    
    
    # se queste non sono presenti utilizza quele create ora (codice sopra) e non fa altro
    file_values_exist = os.path.isfile(file_final_NDWI)
    if (file_values_exist):
        img = gdal.Open(file_final_NDWI, gdal.GA_ReadOnly)
        old_mtx_values = np.array(img.GetRasterBand(1).ReadAsArray())
        img = None     
        # use NDVI mask for NDWI
        out_mtx_mask = tmp_mtx_mask_NDVI 
        out_mtx_A = np.equal(out_mtx_mask, dayoftheyear_images)
        
        ##########################################################################################
        ############# AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA########        
        ##########################################################################################
        out_mtx_A = np.logical_and(np.equal(out_mtx_mask, dayoftheyear_images), SCL_data_validity)       
        ##########################################################################################
        ##########################################################################################  
        
        out_mtx_A = out_mtx_A.astype(int)
        out_mtx_A = np.multiply(out_mtx_A, tmp_mtx_values)
        
        out_mtx_B = np.logical_not(np.equal(out_mtx_mask, dayoftheyear_images))
        
        ##########################################################################################
        ############# AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA########        
        ##########################################################################################        
##        out_mtx_B = np.logical_and(np.equal(out_mtx_mask, dayoftheyear_images), SCL_data_validity)   
        out_mtx_B = np.logical_not(np.logical_and(np.equal(out_mtx_mask, dayoftheyear_images), SCL_data_validity))
        ##########################################################################################
        ##########################################################################################       
        
        out_mtx_B = out_mtx_B.astype(int)
        out_mtx_B = np.multiply(out_mtx_B, old_mtx_values)
        
        out_mtx_values = out_mtx_A + out_mtx_B
        
        out_mtx_A = None
        del out_mtx_A
        out_mtx_B = None
        del out_mtx_B                        
        old_mtx_values = None
        del old_mtx_values
    else:
        print("NO FILES ALREADY AVAILABLE")
        ##########################################################################################
        ############# AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA########        
        ########################################################################################## 
    
        # se sono qui sono alla prima acqusizione del mese
        nodata_mtx = np.full((n_row, n_col), int(nodata))
        # a questo punto SCL_data_validity avro 1 se il valore e da buono, 0 alrimenti. Questo per la prima immagine  disponibile del mese 
        ####fabio 02/09/2021 - tmp_mtx_values = np.add(np.multiply(tmp_mtx_values, SCL_data_validity), np.multiply(nodata_mtx, np.logical_not(SCL_data_validity)))
        out_mtx_values = np.add(np.multiply(tmp_mtx_values, SCL_data_validity), np.multiply(nodata_mtx, np.logical_not(SCL_data_validity)))
        nodata_mtx = None
        del nodata_mtx  
    
        ##########################################################################################
        ##########################################################################################        
        ####fabio 02/09/2021 - out_mtx_values = tmp_mtx_values
     
    
    driver = gdal.GetDriverByName('GTiff')
    outRaster = driver.Create(file_final_NDWI, n_col, n_row, 1, gdal.GDT_Int16)  # GDT_Byte  GDT_Float32
    outRaster.SetGeoTransform((originX, pixelWidth, xskew, originY, yskew, pixelHeight))
    outband = outRaster.GetRasterBand(1)
    outband.WriteArray(out_mtx_values)
    outRasterSRS = osr.SpatialReference()
    outRasterSRS.ImportFromEPSG(32632)
    outRaster.SetProjection(outRasterSRS.ExportToWkt())
    outband.FlushCache()  
    outband = None
    outRaster = None
            
    os.remove(str(file_risultati_NDWI))  
      
    tmp_mtx_values = None
    del tmp_mtx_values     
    out_mtx_values = None
    del out_mtx_values
    out_mtx_mask = None
    del out_mtx_mask        
    ########################################################################################################################################
         
    out_mtx_mask_NDVI = None
    del out_mtx_mask_NDVI   
    tmp_mtx_mask_NDVI = None
    del tmp_mtx_mask_NDVI  
    
    ##########################################################################################
    ############# AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA ## AGGIUNTA########        
    ##########################################################################################
      
    SCL_data_validity = None
    del SCL_data_validity 
   
    ##########################################################################################
    ##########################################################################################        
          
     
    sql = "UPDATE scarichi2 SET processing_step = %s WHERE uuid_prod = %s"
    sqldata = ("Done !! (7/7)", uuid)
    cur.execute(sql, sqldata)                
    conn.commit()
                  
    # update data_elaboration_end   
    tz = pytz.timezone('Europe/Rome')
    elaboration_time_end = datetime.datetime.now(tz)   
    sql = "UPDATE scarichi2 SET data_elaboration_end = %s WHERE uuid_prod = %s"
    sqldata = (elaboration_time_end, uuid)
    cur.execute(sql, sqldata)                
    conn.commit()
        
    for name, size in sorted(((name, sys.getsizeof(value)) for name, value in locals().items()),key= lambda x: -x[1])[:10]:
        print("{:>30}: {:>8}".format(name, sizeof_fmt(size)))

cur.close()
conn.close()
t_totale = (datetime.datetime.now() - t_0).total_seconds()
print ("process ended: " + convertToSeconds(t_totale) + " sec.")

logDB.chiudi(True)
