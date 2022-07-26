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
 
template_wms = '''LAYER
  NAME "$INDICE$_$ANNO$_$MESE$"
  DEBUG 0
  TYPE RASTER
  STATUS ON
  DATA "$VRT_PATH$"
  PROCESSING "BANDS=$BANDS$"
  PROCESSING "$PROCESSING$"
  #PROCESSING "SCALE_1=0,0.421375"
  PROCESSING "RESAMPLE=NEAREST"
  #PROCESSING "LOAD_FULL_RES_IMAGE=YES"
  #PROCESSING "LUT=100:30,160:128,210:200"
  TEMPLATE " "  
  #OFFSITE 0 0 0  
  METADATA
    "ows_title"	"$INDICE$ $ANNO$ $MESE$"
    "ows_abstract"	"$INDICE$ $ANNO$ $MESE$"
    "wms_layer_group" "/$ANNO$"   
    "ows_extent"        "32921.8 4681547.0 810665.2 5280554.3"
    "wms_srs" INCLUDE "/mapserver/outfrmts/srs_all.out"
    "wms_include_items" "all"
    "gml_include_items" "all" 
    "wms_layerlimit" "1"
    INCLUDE "/dati/mapserver/mapfile_443/metadati/$INDICE$.inc"
  END  
  PROJECTION
        "init=epsg:32632"
  END 
  CLASS
    NAME " "
  END    
END''' 
 
template_wms_metainfo_cloudiness_mask = '''LAYER
  NAME "$INDICE$_$ANNO$_$MESE$"
  DEBUG 0
  TYPE RASTER
  STATUS ON
  DATA "$VRT_PATH$"
  PROCESSING "BANDS=$BANDS$"
  PROCESSING "$PROCESSING$"
  PROCESSING "RESAMPLE=NEAREST"
  #PROCESSING "LOAD_FULL_RES_IMAGE=YES"
  #PROCESSING "LUT=100:30,160:128,210:200"
  TEMPLATE " "  
  #OFFSITE 0 0 0  
  METADATA
    "ows_title"	"$INDICE$ $ANNO$ $MESE$"
    "ows_abstract"	"$INDICE$ $ANNO$ $MESE$"
    "wms_layer_group" "/$ANNO$"   
    "ows_extent"        "32921.8 4681547.0 810665.2 5280554.3"
    "wms_srs" INCLUDE "/mapserver/outfrmts/srs_all.out"
    "wms_include_items" "all"
    "gml_include_items" "all" 
    "wms_layerlimit" "1"
    INCLUDE "/dati/mapserver/mapfile_443/metadati/$INDICE$.inc"
  END  
  PROJECTION
        "init=epsg:32632"
  END  
  CLASS
	EXPRESSION ([pixel] = 0)
	NAME "NO DATA"
    STYLE 
	COLOR 1 1 1 
	END
  END  
  CLASS
	EXPRESSION ([pixel] = 1)
	NAME "SATURATED OR DEFECTIVE"
    STYLE 
	COLOR 255 0 0 
	END
  END 
  CLASS
	EXPRESSION ([pixel] = 2)
	NAME "DARK AREA PIXEL"
    STYLE 
	COLOR 90 90 90
	END
  END
  CLASS
	EXPRESSION ([pixel] = 3)
	NAME "CLOUD SHADOWS"
    STYLE 
	COLOR 87 49 52 
	END
  END
  CLASS
	EXPRESSION ([pixel] = 4)
	NAME "VEGETATION"
    STYLE 
	  COLOR 0 255 0 
	END
  END
  CLASS
	EXPRESSION ([pixel] = 5)
	NAME "NOT VEGETATED"
    STYLE 
	COLOR 255 255 0 
	END
  END
  CLASS
	EXPRESSION ([pixel] = 6)
	NAME "WATER"
    STYLE 
	COLOR 0 0 255 
	END
  END
  CLASS
	EXPRESSION ([pixel] = 7)
	NAME "UNCLASSIFIED"
    STYLE 
	COLOR 130 130 130 
	END
  END
  CLASS
	EXPRESSION ([pixel] = 8)
	NAME "CLOUD MEDIUM PROBABILITY"
    STYLE 
	COLOR 160 160 160 
	END
  END
  CLASS
	EXPRESSION ([pixel] = 9)
	NAME "CLOUD HIGH PROBABILITY"
    STYLE 
	COLOR 190 190 190 
	END
  END
  CLASS
	EXPRESSION ([pixel] = 10)
	NAME "THIN CIRRUS"
    STYLE 
	COLOR 0 255 255 
	END
  END
  CLASS
	EXPRESSION ([pixel] = 11)
	NAME "SNOW"
    STYLE 
	COLOR 255 0 255 
	END
  END
END'''  

template_wms_metainfo = '''LAYER
  NAME "$INDICE$_$ANNO$_$MESE$"
  DEBUG 0
  TYPE RASTER
  STATUS ON
  DATA "$VRT_PATH$"
  PROCESSING "BANDS=$BANDS$"
  PROCESSING "$PROCESSING$"
  PROCESSING "RESAMPLE=NEAREST"
  #PROCESSING "LOAD_FULL_RES_IMAGE=YES"
  #PROCESSING "LUT=100:30,160:128,210:200"
  TEMPLATE " "  
  #OFFSITE 0 0 0  
  METADATA
    "ows_title"	"$INDICE$ $ANNO$ $MESE$"
    "ows_abstract"	"$INDICE$ $ANNO$ $MESE$"
    "wms_layer_group" "/$ANNO$"   
    "ows_extent"        "32921.8 4681547.0 810665.2 5280554.3"
    "wms_srs" INCLUDE "/mapserver/outfrmts/srs_all.out"
    "wms_include_items" "all"
    "gml_include_items" "all" 
    "wms_layerlimit" "1"
    INCLUDE "/dati/mapserver/mapfile_443/metadati/$INDICE$.inc"
  END  
  PROJECTION
        "init=epsg:32632"
  END
  CLASS
    NAME " "
  END
END'''



template_wcs = '''LAYER
    NAME "$INDICE$_$ANNO$_$MESE$"
    STATUS ON
    DEBUG 0     
    TYPE RASTER
    DATA "$VRT_PATH$"
    METADATA
      "wcs_label"	"$INDICE$_$ANNO$_$MESE$"  ### required
      "wcs_extent"        "32921.8 4681547.0 810665.2 5280554.3"
      #"wcs_resolution" "10 -10"
      "ows_srs" INCLUDE "/mapserver/outfrmts/srs_all.out"
      "wcs_rangeset_name"   "$INDICE$_$ANNO$_$MESE$"  ### required to support DescribeCoverage request
      "wcs_rangeset_label"  "$INDICE$_$ANNO$_$MESE$" ### required to support DescribeCoverage request
      "wcs_formats" "GEOTIFF_16"
    END    
    PROJECTION
	"init=epsg:32632"
    END  
  END'''
  
template_wcs_multibans3 = '''LAYER
    NAME "$INDICE$_$ANNO$_$MESE$"
    STATUS ON
    DEBUG 0     
    TYPE RASTER
    DATA "$VRT_PATH$"
    METADATA
      "wcs_bandcount" "3"
      "wcs_label"	"$INDICE$_$ANNO$_$MESE$"  ### required
      "wcs_extent"        "32921.8 4681547.0 810665.2 5280554.3"
      #"wcs_resolution" "10 -10"
      "ows_srs" INCLUDE "/mapserver/outfrmts/srs_all.out"
      "wcs_rangeset_name"   "$INDICE$_$ANNO$_$MESE$"  ### required to support DescribeCoverage request
      "wcs_rangeset_label"  "$INDICE$_$ANNO$_$MESE$" ### required to support DescribeCoverage request
      "wcs_formats" "GEOTIFF_16"
    END    
    PROJECTION
	"init=epsg:32632"
    END  
  END'''  

