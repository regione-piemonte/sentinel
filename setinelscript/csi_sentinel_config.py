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

# Proxy Setting
https_proxy = 'https://XXXX:XXX'
http_proxy = 'http://XXXX:XXX'

# Credentials Copernicus Open Access Hub
CSI_user = 'XXXXXX'
CSI_password = 'XXXXXX'

# Search url 
urlSearchApi = "https://scihub.copernicus.eu/dhus/search?vstart=0&rows=100&q="

# url checksum md5
urlMD5 = "https://scihub.copernicus.eu/dhus/odata/v1/Products('%s')/Checksum/Value/$value"

# #### DATABASE ####
dbport =  XXXXX 
dbhost = 'XXXXX'   
dbname = 'XXXXX' 
dbuser = 'XXXXX'
dbpass = 'XXXXX'

#
# #### DOWNLOAD PATH ####
pathDownloadBase = '/sentinel/download/'
pathUnzipBase = '/sentinel/work/'

# #### DEBUG ####
verboseLog = True

########################################################################################################################################


# settings for csi_sentinel_01_search.py and csi_sentinel_01_BIG_search.py
days_before = 7
days_before_big_search = 2400
# http://geojson.io

######################################################################
# POLYGON DATA BASED ON REGIONE PIEMONTE -> PLEASE SET YOUR OWN BBOX #
######################################################################
polygon_data_to_download = [[7.6029679873774585, 43.984768250424594],[9.405549599408264, 43.984768250424594],[9.405549599408264, 46.09675060840081],[7.6029679873774585, 46.09675060840081],[7.6029679873774585, 43.984768250424594]] # 12 tiles

# the symbol '*' in the generic_sentinel_query will be replaced by the time range calculated
#generic_sentinel_query = "platformname:Sentinel-2%20AND%20producttype:S2MSI2A%20AND%20ingestiondate:[*]%20AND%20footprint:%22intersects(POLYGON((*!)))%22&orderby=ingestiondate desc"
#generic_sentinel_query_old = "platformname:Sentinel-2%20AND%20producttype:S2MSI2AP%20AND%20ingestiondate:[*]%20AND%20footprint:%22intersects(POLYGON((*!)))%22&orderby=ingestiondate desc"
old_generic_sentinel_query = "platformname:Sentinel-2%20AND%20producttype:S2MSI2A%20AND%beginposition:[*]%20AND%20footprint:%22intersects(POLYGON((*!)))%22&orderby=beginposition desc"
old_generic_sentinel_query_old = "platformname:Sentinel-2%20AND%20producttype:S2MSI2AP%20AND%beginposition:[*]%20AND%20footprint:%22intersects(POLYGON((*!)))%22&orderby=ingestiondate desc"
generic_sentinel_query =    "platformname:Sentinel-2%20AND%20producttype:S2MSI2A%20AND%20ingestiondate:[*]%20AND%20footprint:%22intersects(POLYGON((*!)))%22&orderby=ingestiondate desc"
generic_sentinel_query_old =    "platformname:Sentinel-2%20AND%20producttype:S2MSI2AP%20AND%20ingestiondate:[*]%20AND%20footprint:%22intersects(POLYGON((*!)))%22&orderby=ingestiondate desc"


# settings for csi_sentinel_04_delete_old_zip.py
delete_zip_older_than_x_days = 120 # number of days before the current datetime. The zip file downloaded this number of days before and already used will be erased                                 

# settings for csi_sentinel_03_processing.py
nr_processing_block = 4 # this parameters is used to select the number of processing step (entry in the db) are processed each time scrip is in execution
# setting the values (in this list) to be considered for the cloudiness
cloudiness_class_bad = [1,8,9,10] # 3 = cloud_shadow - 8 = cloud_medium_probability - 9 = cloud_high_probability - 10 = thin_cirrus
cloudiness_class_good = [2,3,4,5,6,7,11] # 
cloudiness_class_nodata = [0] #

# settings for csi_sentinel_04_download.py
nr_download_block = 5
th_max_tentativi_download = 220 

# settings for csi_sentinel_05_organize.py
pathSaveFinalImageBase = '/sentinel/output/images/'

# settings for csi_sentinel_06_createIMG.py
pathSaveTriMonthImageBase = '/sentinel/output/images/trimonth'

# settings for csi_sentinel_06_createIMG.py
referenceTile = "T32TLQ"
th_novalue_red_channel = 6028020 # no value < 6028020 means less than 5%                       
th_sd_red_channel = 0 # 0 means the standard deviation of the images will not be used
th_days_around_reference_day = 60 # 60 means 2 months. In case the reference is at the and of the third month, the others have to be on the second or third month
listMonthsAsReference = [2,5,8,11]


# settings for WMS and WCS services
# NEW FOR VRT
include_ogc_path = '/sentinel/output/include_ogc'
static_mapfile_wcs_NDVI = '/sentinel/output/include_ogc/NDVI/wcs_layers.inc'
static_mapfile_wcs_EVI = '/sentinel/output/include_ogc/EVI/wcs_layers.inc'
static_mapfile_wcs_NBR = '/sentinel/output/include_ogc/NBR/wcs_layers.inc'
static_mapfile_wcs_NDWI = '/sentinel/output/include_ogc/NDWI/wcs_layers.inc'
static_mapfile_wcs_TC_4_3_2 = '/sentinel/output/include_ogc/TC_4-3-2/wcs_layers.inc'
static_mapfile_wcs_SC_8_4_3 = '/sentinel/output/include_ogc/SC_8-4-3/wcs_layers.inc'
static_mapfile_wcs_SC_8_11_4 = '/sentinel/output/include_ogc/SC_8-11-4/wcs_layers.inc'
static_mapfile_wcs_metainfo = '/sentinel/output/include_ogc/metainfo/wcs_layers.inc'
static_mapfile_wcs_delta_NBR = '/sentinel/output/include_ogc/delta_NBR/wcs_layers.inc'
static_mapfile_wcs_annual_delta_NBR = '/sentinel/output/include_ogc/annual_delta_NBR/wcs_layers.inc'

static_mapfile_wms_NDVI = '/sentinel/output/include_ogc/NDVI/wms_layers.inc'
static_mapfile_wms_EVI = '/sentinel/output/include_ogc/EVI/wms_layers.inc'
static_mapfile_wms_NBR = '/sentinel/output/include_ogc/NBR/wms_layers.inc'
static_mapfile_wms_NDWI = '/sentinel/output/include_ogc/NDWI/wms_layers.inc'
static_mapfile_wms_TC_4_3_2 = '/sentinel/output/include_ogc/TC_4-3-2/wms_layers.inc'
static_mapfile_wms_SC_8_4_3 = '/sentinel/output/include_ogc/SC_8-4-3/wms_layers.inc'
static_mapfile_wms_SC_8_11_4 = '/sentinel/output/include_ogc/SC_8-11-4/wms_layers.inc'
static_mapfile_wms_metainfo = '/sentinel/output/include_ogc/metainfo/wms_layers.inc'
static_mapfile_wms_delta_NBR = '/sentinel/output/include_ogc/delta_NBR/wms_layers.inc'
static_mapfile_wms_annual_delta_NBR = '/sentinel/output/include_ogc/annual_delta_NBR/wms_layers.inc'
# NEW FOR VRT

# this variable are necessary to be setted because there are not information to calculate the delta NBR for the last acquired month (no previou data available)
first_data_available_year = 2017
first_data_available_month = 4

# variable for annual deltaNBR
first_data_available_year_annualNBR = 2017
month_annualNBR = [7,8]

####################### VARIABILI TEMPORANEE ########################
#coinvolge file organize, deltaNBR, annual_deltaNBR, csi_sentinel_06_createIMG_test
# variable temporanee per evitare di ricreare file inc rielaborando nuovamente file che gli hanno gia creati
# se i file sono di un periodo successivo o uguale a questa data creo anche gli inc
year_before_inc_file_already_available = 2021
month_before_inc_file_already_available = 9
  
#coinvolge file organize e csi_sentinel_06_createIMG_test.py
# variable temporanee per creazione cartella backup se dati in output gia disponibili
# se devo organizzare un dato precedente a questa data (ho gia dei dati in output per essa) creo una copia di backup di questi
year_before_images_available = 2021    
month_before_images_available = 9  
####################################################################